package org.apache.ignite.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.util.Progressable;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.hadoop.fs.HadoopFileSystemFactory;
import org.apache.ignite.hadoop.fs.IgniteHadoopIgfsSecondaryFileSystem;
import org.apache.ignite.hadoop.fs.v1.IgniteHadoopFileSystem;
import org.apache.ignite.igfs.*;
import org.apache.ignite.internal.igfs.common.IgfsLogger;
import org.apache.ignite.internal.processors.hadoop.delegate.HadoopDelegateUtils;
import org.apache.ignite.internal.processors.hadoop.delegate.HadoopFileSystemFactoryDelegate;
import org.apache.ignite.internal.processors.hadoop.impl.igfs.*;
import org.apache.ignite.internal.processors.igfs.IgfsHandshakeResponse;
import org.apache.ignite.internal.processors.igfs.IgfsModeResolver;
import org.apache.ignite.internal.processors.igfs.IgfsPaths;
import org.apache.ignite.internal.processors.igfs.IgfsUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.ignite.IgniteFileSystem.IGFS_SCHEME;
import static org.apache.ignite.configuration.FileSystemConfiguration.DFLT_IGFS_LOG_BATCH_SIZE;
import static org.apache.ignite.configuration.FileSystemConfiguration.DFLT_IGFS_LOG_DIR;
import static org.apache.ignite.igfs.IgfsMode.PROXY;
import static org.apache.ignite.internal.processors.hadoop.impl.fs.HadoopParameters.*;
import static org.apache.ignite.internal.processors.hadoop.impl.fs.HadoopParameters.PARAM_IGFS_LOG_BATCH_SIZE;
import static org.apache.ignite.internal.processors.hadoop.impl.fs.HadoopParameters.PARAM_IGFS_LOG_ENABLED;
import static org.apache.ignite.internal.processors.hadoop.impl.igfs.HadoopIgfsUtils.parameter;

/**
 * Created by masayuki on 2017/03/25.
 */
public class IgniteHadoopDistributedFileSystem extends DistributedFileSystem {

    /** Empty array of file block locations. */
    private static final BlockLocation[] EMPTY_BLOCK_LOCATIONS = new BlockLocation[0];

    /** Empty array of file statuses. */
    public static final FileStatus[] EMPTY_FILE_STATUS = new FileStatus[0];

    /** Ensures that close routine is invoked at most once. */
    private final AtomicBoolean closeGuard = new AtomicBoolean();

    /** Grid remote client. */
    private HadoopIgfsWrapper rmtClient;

    /** working directory. */
    private Path workingDir;

    /** Default replication factor. */
    private short dfltReplication;

    /** Base file system uri. */
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    private URI uri;

    /** Authority. */
    private String uriAuthority;

    /** Client logger. */
    private IgfsLogger clientLog;

    /** Secondary URI string. */
    private URI secondaryUri;

    /** The user name this file system was created on behalf of. */
    private String user;

    /** IGFS mode resolver. */
    private IgfsModeResolver modeRslvr;

    /** The secondary file system factory. */
    private HadoopFileSystemFactoryDelegate factory;

    /** Management connection flag. */
    private boolean mgmt;

    /** Whether custom sequential reads before prefetch value is provided. */
    private boolean seqReadsBeforePrefetchOverride;

    /** IGFS group block size. */
    private long igfsGrpBlockSize;

    /** Flag that controls whether file writes should be colocated. */
    private boolean colocateFileWrites;

    /** Prefer local writes. */
    private boolean preferLocFileWrites;

    /** Custom-provided sequential reads before prefetch. */
    private int seqReadsBeforePrefetch;

    /**
     * Enter busy state.
     *
     * @throws IOException If file system is stopped.
     */
    private void enterBusy() throws IOException {
        if (closeGuard.get())
            throw new IOException("File system is stopped.");
    }

    /**
     * Leave busy state.
     */
    private void leaveBusy() {
        // No-op.
    }

    /** {@inheritDoc} */
    @SuppressWarnings("ConstantConditions")
    @Override public void initialize(URI name, Configuration cfg) throws IOException {
        System.out.println("initialize");


        try {
            if (rmtClient != null)
                throw new IOException("File system is already initialized: " + rmtClient);

            A.notNull(name, "name");
            A.notNull(cfg, "cfg");

            setConf(cfg);
            mgmt = cfg.getBoolean(IgniteHadoopFileSystem.IGFS_MANAGEMENT, false);

            uri = name;
            uriAuthority = uri.getAuthority();
            user = IgniteHadoopFileSystem.getFsHadoopUser();

            // Override sequential reads before prefetch if needed.
            seqReadsBeforePrefetch = parameter(cfg, PARAM_IGFS_SEQ_READS_BEFORE_PREFETCH, uriAuthority, 0);
            if (seqReadsBeforePrefetch > 0)
                seqReadsBeforePrefetchOverride = true;

            // In Ignite replication factor is controlled by data cache affinity.
            // We use replication factor to force the whole file to be stored on local node.
            dfltReplication = (short) cfg.getInt("dfs.replication", 3);

            // Get file colocation control flag.
            colocateFileWrites = parameter(cfg, PARAM_IGFS_COLOCATED_WRITES, uriAuthority, false);
            preferLocFileWrites = cfg.getBoolean(PARAM_IGFS_PREFER_LOCAL_WRITES, false);

            // Get log directory.
            String logDirCfg = parameter(cfg, PARAM_IGFS_LOG_DIR, uriAuthority, DFLT_IGFS_LOG_DIR);
            File logDirFile = U.resolveIgnitePath(logDirCfg);
            String logDir = logDirFile != null ? logDirFile.getAbsolutePath() : null;

            rmtClient = new HadoopIgfsWrapper(uriAuthority, logDir, cfg, LOG, user);
            IgfsHandshakeResponse handshake = rmtClient.handshake(logDir);
            igfsGrpBlockSize = handshake.blockSize();
            IgfsPaths paths = handshake.secondaryPaths();

            // Initialize client logger.
            Boolean logEnabled = parameter(cfg, PARAM_IGFS_LOG_ENABLED, uriAuthority, false);

            if (handshake.sampling() != null ? handshake.sampling() : logEnabled) {
                if (logDir == null)
                    throw new IOException("Failed to resolve log directory: " + logDirCfg);
                Integer batchSize = parameter(cfg, PARAM_IGFS_LOG_BATCH_SIZE, uriAuthority, DFLT_IGFS_LOG_BATCH_SIZE);
                clientLog = IgfsLogger.logger(uriAuthority, handshake.igfsName(), logDir, batchSize);
            } else {
                clientLog = IgfsLogger.disabledLogger();
            }

            try {
                modeRslvr = new IgfsModeResolver(paths.defaultMode(), paths.pathModes());
            } catch (IgniteCheckedException ice) {
                throw new IOException(ice);
            }
            boolean initSecondary = paths.defaultMode() == PROXY;

            if (!initSecondary && paths.pathModes() != null && !paths.pathModes().isEmpty()) {
                for (T2<IgfsPath, IgfsMode> pathMode : paths.pathModes()) {
                    IgfsMode mode = pathMode.getValue();
                    if (mode == PROXY) {
                        initSecondary = true;
                        break;
                    }
                }
            }

            if (initSecondary) {
                try {
                    HadoopFileSystemFactory factory0 =
                            (HadoopFileSystemFactory) paths.getPayload(getClass().getClassLoader());
                    factory = HadoopDelegateUtils.fileSystemFactoryDelegate(getClass().getClassLoader(), factory0);
                } catch (IgniteCheckedException e) {
                    throw new IOException("Failed to get secondary file system factory.", e);
                }

                if (factory == null)
                    throw new IOException("Failed to get secondary file system factory (did you set " +
                            IgniteHadoopIgfsSecondaryFileSystem.class.getName() + " as \"secondaryFIleSystem\" in " +
                            FileSystemConfiguration.class.getName() + "?)");

                factory.start();

                try {
                    FileSystem secFs = (FileSystem) factory.get(user);
                    secondaryUri = secFs.getUri();
                    A.ensure(secondaryUri != null, "Secondary file system uri should not be null.");
                } catch (IOException e) {
                    if (!mgmt)
                        throw new IOException("Failed to connect to the secondary file system: " + secondaryUri, e);
                    else
                        LOG.warn("Visor failed to create secondary file system (operations on paths with PROXY mode " +
                                "will have no effect): " + e.getMessage());
                }
            }

            // set working directory to the home directory of the current Fs user:
            setWorkingDirectory(null);
        } finally {
            leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override public FSDataInputStream open(Path f, int bufSize) throws IOException {
        A.notNull(f, "f");

        enterBusy();

        try {
            IgfsPath path = convert(f);
            IgfsMode mode = mode(path);

            HadoopIgfsStreamDelegate stream = seqReadsBeforePrefetchOverride ?
                    rmtClient.open(path, seqReadsBeforePrefetch) : rmtClient.open(path);

            long logId = -1;

            if (clientLog.isLogEnabled()) {
                logId = IgfsLogger.nextId();

                clientLog.logOpen(logId, path, mode, bufSize, stream.length());
            }

            if (LOG.isDebugEnabled())
                LOG.debug("Opening input stream [thread=" + Thread.currentThread().getName() + ", path=" + path +
                        ", bufSize=" + bufSize + ']');

            HadoopIgfsInputStream igfsIn = new HadoopIgfsInputStream(stream, stream.length(),
                    bufSize, LOG, clientLog, logId);

            if (LOG.isDebugEnabled())
                LOG.debug("Opened input stream [path=" + path + ", delegate=" + stream + ']');

            return new FSDataInputStream(igfsIn);
        } finally {
            leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("deprecation")
    @Override public FSDataOutputStream create(Path f, final FsPermission perm, boolean overwrite, int bufSize,
                                               short replication, long blockSize, Progressable progress) throws IOException {
        A.notNull(f, "f");

        enterBusy();

        OutputStream out = null;

        try {
            IgfsPath path = convert(f);
            IgfsMode mode = mode(path);

            if (LOG.isDebugEnabled())
                LOG.debug("Opening output stream in create [thread=" + Thread.currentThread().getName() + "path=" +
                        path + ", overwrite=" + overwrite + ", bufSize=" + bufSize + ']');

            if (mode == PROXY) {
                final FileSystem secondaryFs = secondaryFileSystem();

                if (secondaryFs == null) {
                    assert mgmt;

                    throw new IOException("Failed to create file (secondary file system is not initialized): " + f);
                }

                FSDataOutputStream os =
                        secondaryFs.create(toSecondary(f), perm, overwrite, bufSize, replication, blockSize, progress);

                if (clientLog.isLogEnabled()) {
                    long logId = IgfsLogger.nextId();

                    clientLog.logCreate(logId, path, PROXY, overwrite, bufSize, replication, blockSize);

                    return new FSDataOutputStream(new HadoopIgfsProxyOutputStream(os, clientLog, logId));
                }
                else
                    return os;
            }
            else {
                Map<String,String> propMap = permission(perm);

                propMap.put(IgfsUtils.PROP_PREFER_LOCAL_WRITES, Boolean.toString(preferLocFileWrites));

                // Create stream and close it in the 'finally' section if any sequential operation failed.
                HadoopIgfsStreamDelegate stream = rmtClient.create(path, overwrite, colocateFileWrites,
                        replication, blockSize, propMap);

                assert stream != null;

                long logId = -1;

                if (clientLog.isLogEnabled()) {
                    logId = IgfsLogger.nextId();

                    clientLog.logCreate(logId, path, mode, overwrite, bufSize, replication, blockSize);
                }

                if (LOG.isDebugEnabled())
                    LOG.debug("Opened output stream in create [path=" + path + ", delegate=" + stream + ']');

                HadoopIgfsOutputStream igfsOut = new HadoopIgfsOutputStream(stream, LOG, clientLog,
                        logId);

                bufSize = Math.max(64 * 1024, bufSize);

                out = new BufferedOutputStream(igfsOut, bufSize);

                FSDataOutputStream res = new FSDataOutputStream(out, null, 0);

                // Mark stream created successfully.
                out = null;

                return res;
            }
        }
        finally {
            // Close if failed during stream creation.
            if (out != null)
                U.closeQuiet(out);

            leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public boolean rename(Path src, Path dst) throws IOException {
        A.notNull(src, "src");
        A.notNull(dst, "dst");

        enterBusy();

        try {
            IgfsPath srcPath = convert(src);
            IgfsPath dstPath = convert(dst);
            IgfsMode mode = mode(srcPath);

            if (clientLog.isLogEnabled())
                clientLog.logRename(srcPath, mode, dstPath);

            try {
                rmtClient.rename(srcPath, dstPath);
            } catch (IOException ioe) {
                // Log the exception before rethrowing since it may be ignored:
                LOG.warn("Failed to rename [srcPath=" + srcPath + ", dstPath=" + dstPath + ", mode=" + mode + ']', ioe);
                throw ioe;
            }
            return true;
        } catch (IOException e) {
            // Intentionally ignore IGFS exceptions here to follow Hadoop contract.
            if (F.eq(IOException.class, e.getClass()) && (e.getCause() == null ||
                    !X.hasCause(e.getCause(), IgfsException.class)))
                throw e;
            else
                return false;
        } finally {
            leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("deprecation")
    @Override public boolean delete(Path f) throws IOException {
        return delete(f, false);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public boolean delete(Path f, boolean recursive) throws IOException {
        A.notNull(f, "f");

        enterBusy();

        try {
            IgfsPath path = convert(f);
            IgfsMode mode = mode(path);

            // Will throw exception if delete failed.
            boolean res = rmtClient.delete(path, recursive);
            if (clientLog.isLogEnabled())
                clientLog.logDelete(path, mode, recursive);
            return res;
        } catch (IOException e) {
            // Intentionally ignore IGFS exceptions here to follow Hadoop contract.
            if (F.eq(IOException.class, e.getClass()) && (e.getCause() == null ||
                    !X.hasCause(e.getCause(), IgfsException.class)))
                throw e;
            else
                return false;
        } finally {
            leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override public FileStatus[] listStatus(Path f) throws IOException {
        System.out.println("listStatus");
        A.notNull(f, "f");

        enterBusy();

        try {
            IgfsPath path = convert(f);
            IgfsMode mode = mode(path);

            Collection<IgfsFile> list = rmtClient.listFiles(path);
            if (list == null)
                throw new FileNotFoundException("File " + f + " does not exist.");

            List<IgfsFile> files = new ArrayList<>(list);
            FileStatus[] arr = new FileStatus[files.size()];
            for (int i = 0; i < arr.length; i++)
                arr[i] = convert(files.get(i));

            if (clientLog.isLogEnabled()) {
                String[] fileArr = new String[arr.length];
                for (int i = 0; i < arr.length; i++)
                    fileArr[i] = arr[i].getPath().toString();
                clientLog.logListDirectory(path, mode, fileArr);
            }
            return arr;
        } finally {
            leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override public Path getHomeDirectory() {
        Path path = new Path("/user/" + user);
        return path.makeQualified(getUri(), null);
    }

    /** {@inheritDoc} */
    @Override public void setWorkingDirectory(Path newPath) {
        try {
            if (newPath == null) {
                Path homeDir = getHomeDirectory();
                FileSystem secondaryFs  = secondaryFileSystem();
                if (secondaryFs != null)
                    secondaryFs.setWorkingDirectory(toSecondary(homeDir));
                workingDir = homeDir;
            }
            else {
                Path fixedNewPath = fixRelativePart(newPath);
                String res = fixedNewPath.toUri().getPath();
                if (!DFSUtil.isValidName(res))
                    throw new IllegalArgumentException("Invalid DFS directory name " + res);
                FileSystem secondaryFs  = secondaryFileSystem();
                if (secondaryFs != null)
                    secondaryFs.setWorkingDirectory(toSecondary(fixedNewPath));
                workingDir = fixedNewPath;
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to obtain secondary file system instance.", e);
        }
    }

    /** {@inheritDoc} */
    @Override public Path getWorkingDirectory() {
        return workingDir;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public boolean mkdirs(Path f, FsPermission perm) throws IOException {
        A.notNull(f, "f");

        enterBusy();

        try {
            IgfsPath path = convert(f);
            IgfsMode mode = mode(path);

            boolean mkdirRes = rmtClient.mkdirs(path, permission(perm));
            if (clientLog.isLogEnabled())
                clientLog.logMakeDirectory(path, mode);
            return mkdirRes;
        } catch (IOException e) {
            // Intentionally ignore IGFS exceptions here to follow Hadoop contract.
            if (F.eq(IOException.class, e.getClass()) && (e.getCause() == null ||
                    !X.hasCause(e.getCause(), IgfsException.class)))
                throw e;
            else
                return false;
        } finally {
            leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override public FileStatus getFileStatus(Path f) throws IOException {
        System.out.println("getFileStatus");
        A.notNull(f, "f");

        enterBusy();

        try {
            IgfsFile info = rmtClient.info(convert(f));
            if (info == null)
                throw new FileNotFoundException("File not found: " + f);
            return convert(info);
        } finally {
            leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override public ContentSummary getContentSummary(Path f) throws IOException {
        A.notNull(f, "f");

        enterBusy();

        try {
            IgfsPathSummary sum = rmtClient.contentSummary(convert(f));
            return new ContentSummary(sum.totalLength(), sum.filesCount(), sum.directoriesCount(),
                    -1, sum.totalLength(), rmtClient.fsStatus().spaceTotal());
        } finally {
            leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override public BlockLocation[] getFileBlockLocations(FileStatus status, long start, long len) throws IOException {
        A.notNull(status, "status");

        enterBusy();

        try {
            IgfsPath path = convert(status.getPath());
            long now = System.currentTimeMillis();

            List<IgfsBlockLocation> affinity = new ArrayList<>(rmtClient.affinity(path, start, len));
            BlockLocation[] arr = new BlockLocation[affinity.size()];
            for (int i = 0; i < arr.length; i++)
                arr[i] = convert(affinity.get(i));

            if (LOG.isDebugEnabled())
                LOG.debug("Fetched file locations [path=" + path + ", fetchTime=" +
                        (System.currentTimeMillis() - now) + ", locations=" + Arrays.asList(arr) + ']');
            return arr;
        }
        catch (FileNotFoundException ignored) {
            return EMPTY_BLOCK_LOCATIONS;
        }
        finally {
            leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("deprecation")
    @Override public long getDefaultBlockSize() {
        return igfsGrpBlockSize;
    }

    /**
     * Resolve path mode.
     *
     * @param path HDFS path.
     * @return Path mode.
     */
    public IgfsMode mode(Path path) {
        return mode(convert(path));
    }

    /**
     * Resolve path mode.
     *
     * @param path IGFS path.
     * @return Path mode.
     */
    public IgfsMode mode(IgfsPath path) {
        return modeRslvr.resolveMode(path);
    }

    /**
     * @return {@code true} If secondary file system is initialized.
     */
    public boolean hasSecondaryFileSystem() {
        return factory != null;
    }

    /**
     * Convert the given path to path acceptable by the primary file system.
     *
     * @param path Path.
     * @return Primary file system path.
     */
    private Path toPrimary(Path path) {
        return convertPath(path, uri);
    }

    /**
     * Convert the given path to path acceptable by the secondary file system.
     *
     * @param path Path.
     * @return Secondary file system path.
     */
    private Path toSecondary(Path path) {
        assert factory != null;
        assert secondaryUri != null;

        return convertPath(path, secondaryUri);
    }

    /**
     * Convert path using the given new URI.
     *
     * @param path Old path.
     * @param newUri New URI.
     * @return New path.
     */
    private Path convertPath(Path path, URI newUri) {
        assert newUri != null;

        if (path != null) {
            URI pathUri = path.toUri();

            try {
                return new Path(new URI(pathUri.getScheme() != null ? newUri.getScheme() : null,
                        pathUri.getAuthority() != null ? newUri.getAuthority() : null, pathUri.getPath(), null, null));
            }
            catch (URISyntaxException e) {
                throw new IgniteException("Failed to construct secondary file system path from the primary file " +
                        "system path: " + path, e);
            }
        }
        else
            return null;
    }

    /**
     * Convert a file status obtained from the secondary file system to a status of the primary file system.
     *
     * @param status Secondary file system status.
     * @return Primary file system status.
     */
    @SuppressWarnings("deprecation")
    private FileStatus toPrimary(FileStatus status) {
        return status != null ? new FileStatus(status.getLen(), status.isDir(), status.getReplication(),
                status.getBlockSize(), status.getModificationTime(), status.getAccessTime(), status.getPermission(),
                status.getOwner(), status.getGroup(), toPrimary(status.getPath())) : null;
    }

    /**
     * Convert IGFS path into Hadoop path.
     *
     * @param path IGFS path.
     * @return Hadoop path.
     */
    private Path convert(IgfsPath path) {
        return new Path(IGFS_SCHEME, uriAuthority, path.toString());
    }

    /**
     * Convert Hadoop path into IGFS path.
     *
     * @param path Hadoop path.
     * @return IGFS path.
     */
    @Nullable
    private IgfsPath convert(@Nullable Path path) {
        if (path == null)
            return null;

        return path.isAbsolute() ? new IgfsPath(path.toUri().getPath()) :
                new IgfsPath(convert(workingDir), path.toUri().getPath());
    }

    /**
     * Convert IGFS affinity block location into Hadoop affinity block location.
     *
     * @param block IGFS affinity block location.
     * @return Hadoop affinity block location.
     */
    private BlockLocation convert(IgfsBlockLocation block) {
        Collection<String> names = block.names();
        Collection<String> hosts = block.hosts();

        return new BlockLocation(
                names.toArray(new String[names.size()]) /* hostname:portNumber of data nodes */,
                hosts.toArray(new String[hosts.size()]) /* hostnames of data nodes */,
                block.start(), block.length()
        ) {
            @Override public String toString() {
                try {
                    return "BlockLocation [offset=" + getOffset() + ", length=" + getLength() +
                            ", hosts=" + Arrays.asList(getHosts()) + ", names=" + Arrays.asList(getNames()) + ']';
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    /**
     * Convert IGFS file information into Hadoop file status.
     *
     * @param file IGFS file information.
     * @return Hadoop file status.
     */
    @SuppressWarnings("deprecation")
    private FileStatus convert(IgfsFile file) {
        return new FileStatus(
                file.length(),
                file.isDirectory(),
                getDefaultReplication(),
                file.groupBlockSize(),
                file.modificationTime(),
                file.accessTime(),
                permission(file),
                file.property(IgfsUtils.PROP_USER_NAME, user),
                file.property(IgfsUtils.PROP_GROUP_NAME, "users"),
                convert(file.path())) {
            @Override public String toString() {
                return "FileStatus [path=" + getPath() + ", isDir=" + isDir() + ", len=" + getLen() +
                        ", mtime=" + getModificationTime() + ", atime=" + getAccessTime() + ']';
            }
        };
    }

    /**
     * Convert Hadoop permission into IGFS file attribute.
     *
     * @param perm Hadoop permission.
     * @return IGFS attributes.
     */
    private Map<String, String> permission(FsPermission perm) {
        if (perm == null)
            perm = FsPermission.getDefault();

        return F.asMap(IgfsUtils.PROP_PERMISSION, toString(perm));
    }

    /**
     * @param perm Permission.
     * @return String.
     */
    private static String toString(FsPermission perm) {
        return String.format("%04o", perm.toShort());
    }

    /**
     * Convert IGFS file attributes into Hadoop permission.
     *
     * @param file File info.
     * @return Hadoop permission.
     */
    private FsPermission permission(IgfsFile file) {
        String perm = file.property(IgfsUtils.PROP_PERMISSION, null);

        if (perm == null)
            return FsPermission.getDefault();

        try {
            return new FsPermission((short)Integer.parseInt(perm, 8));
        }
        catch (NumberFormatException ignore) {
            return FsPermission.getDefault();
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteHadoopDistributedFileSystem.class, this);
    }

    /**
     * Returns the user name this File System is created on behalf of.
     * @return the user name
     */
    public String user() {
        return user;
    }

    /**
     * Gets cached or creates a {@link FileSystem}.
     *
     * @return The secondary file system.
     */
    private @Nullable FileSystem secondaryFileSystem() throws IOException{
        if (factory == null)
            return null;

        return (FileSystem)factory.get(user);
    }
}
