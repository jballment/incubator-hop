/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.core.vfs;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.vfs2.CacheStrategy;
import org.apache.commons.vfs2.FileContent;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.cache.SoftRefFilesCache;
import org.apache.commons.vfs2.impl.DefaultFileReplicator;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.commons.vfs2.impl.FileContentInfoFilenameFactory;
import org.apache.commons.vfs2.impl.StandardFileSystemManager;
import org.apache.commons.vfs2.provider.FileProvider;
import org.apache.commons.vfs2.provider.local.LocalFile;
import org.apache.commons.vfs2.util.Cryptor;
import org.apache.commons.vfs2.util.CryptorFactory;
import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopFileException;
import org.apache.hop.core.plugins.IPlugin;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.plugin.IVfs;
import org.apache.hop.core.vfs.plugin.VfsPluginType;
import org.apache.hop.i18n.BaseMessages;

public class HopVfs {
  private static final Class<?> PKG = HopVfs.class;

  public static final String TEMP_DIR = System.getProperty("java.io.tmpdir");

  private static DefaultFileSystemManager fsm;
  private static DefaultFileSystemManager extendedFsm;

  private static final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  public static DefaultFileSystemManager getFileSystemManager() {
    lock.readLock().lock();
    try {
      if (fsm == null) {
        try {
          fsm = createFileSystemManager();
          fsm.init();
        } catch (Exception e) {
          throw new RuntimeException("Error initializing file system manager : ", e);
        }
      }
      return fsm;
    } finally {
      lock.readLock().unlock();
    }
  }

  public static DefaultFileSystemManager getFileSystemManager(IVariables variables) {
    lock.readLock().lock();
    try {
      if (extendedFsm == null) {
        try {
          extendedFsm = createFileSystemManager();
          // Here are extra VFS plugins to register
          //
          PluginRegistry registry = PluginRegistry.getInstance();
          List<IPlugin> plugins = registry.getPlugins(VfsPluginType.class);
          for (IPlugin plugin : plugins) {
            IVfs iVfs = registry.loadClass(plugin, IVfs.class);
            try {
              Map<String, FileProvider> fileProviderMap = iVfs.getProviders(variables);
              if (fileProviderMap != null) {
                for (Map.Entry<String, FileProvider> entry : fileProviderMap.entrySet()) {
                  extendedFsm.addProvider(entry.getKey(), entry.getValue());
                }
              }
            } catch (Exception e) {
              throw new HopException(
                  "Error registering provider for VFS plugin "
                      + plugin.getIds()[0]
                      + " : "
                      + plugin.getName()
                      + " : ",
                  e);
            }
          }

          extendedFsm.init();
        } catch (Exception e) {
          throw new RuntimeException("Error initializing file system manager : ", e);
        }
      }
      return extendedFsm;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Make sure to close when done using!
   *
   * @return A new standard file system manager
   * @throws HopException
   */
  private static DefaultFileSystemManager createFileSystemManager() throws HopException {
    try {
      DefaultFileSystemManager fsm = new DefaultFileSystemManager();
      fsm.addProvider("ram", new org.apache.commons.vfs2.provider.ram.RamFileProvider());
      fsm.addProvider(
          "file", new org.apache.commons.vfs2.provider.local.DefaultLocalFileProvider());
      fsm.addProvider("res", new org.apache.commons.vfs2.provider.res.ResourceFileProvider());
      fsm.addProvider("zip", new org.apache.commons.vfs2.provider.zip.ZipFileProvider());
      fsm.addProvider("gz", new org.apache.commons.vfs2.provider.gzip.GzipFileProvider());
      fsm.addProvider("jar", new org.apache.commons.vfs2.provider.jar.JarFileProvider());
      fsm.addProvider("http", new org.apache.commons.vfs2.provider.http5.Http5FileProvider());
      fsm.addProvider("https", new org.apache.commons.vfs2.provider.http5s.Http5sFileProvider());
      fsm.addProvider("ftp", new org.apache.commons.vfs2.provider.ftp.FtpFileProvider());
      fsm.addProvider("ftps", new org.apache.commons.vfs2.provider.ftps.FtpsFileProvider());
      fsm.addProvider("sftp", new org.apache.commons.vfs2.provider.sftp.SftpFileProvider());
      fsm.addProvider("war", new org.apache.commons.vfs2.provider.jar.JarFileProvider());
      fsm.addProvider("par", new org.apache.commons.vfs2.provider.jar.JarFileProvider());
      fsm.addProvider("ear", new org.apache.commons.vfs2.provider.jar.JarFileProvider());
      fsm.addProvider("sar", new org.apache.commons.vfs2.provider.jar.JarFileProvider());
      fsm.addProvider("ejb3", new org.apache.commons.vfs2.provider.jar.JarFileProvider());
      fsm.addProvider("tmp", new org.apache.commons.vfs2.provider.temp.TemporaryFileProvider());
      fsm.addProvider("tar", new org.apache.commons.vfs2.provider.tar.TarFileProvider());
      fsm.addProvider("tbz2", new org.apache.commons.vfs2.provider.tar.TarFileProvider());
      fsm.addProvider("tgz", new org.apache.commons.vfs2.provider.tar.TarFileProvider());
      fsm.addProvider("bz2", new org.apache.commons.vfs2.provider.bzip2.Bzip2FileProvider());
      fsm.addProvider(
          "files-cache", new org.apache.commons.vfs2.provider.temp.TemporaryFileProvider());
      fsm.addExtensionMap("jar", "jar");
      fsm.addExtensionMap("zip", "zip");
      fsm.addExtensionMap("gz", "gz");
      fsm.addExtensionMap("tar", "tar");
      fsm.addExtensionMap("tbz2", "tar");
      fsm.addExtensionMap("tgz", "tar");
      fsm.addExtensionMap("bz2", "bz2");
      fsm.addMimeTypeMap("application/x-tar", "tar");
      fsm.addMimeTypeMap("application/x-gzip", "gz");
      fsm.addMimeTypeMap("application/zip", "zip");
      fsm.setFileContentInfoFactory(new FileContentInfoFilenameFactory());
      fsm.setReplicator(new DefaultFileReplicator());

      fsm.setFilesCache(new SoftRefFilesCache());
      fsm.setCacheStrategy(CacheStrategy.ON_RESOLVE);

      // Here are extra VFS plugins to register
      //
      PluginRegistry registry = PluginRegistry.getInstance();
      List<IPlugin> plugins = registry.getPlugins(VfsPluginType.class);
      for (IPlugin plugin : plugins) {
        IVfs iVfs = registry.loadClass(plugin, IVfs.class);
        try {
          fsm.addProvider(iVfs.getUrlSchemes(), iVfs.getProvider());
        } catch (Exception e) {
          throw new HopException(
              "Error registering provider for VFS plugin "
                  + plugin.getIds()[0]
                  + " : "
                  + plugin.getName()
                  + " : ",
              e);
        }
      }
      return fsm;
    } catch (Exception e) {
      throw new HopException("Error creating file system manager", e);
    }
  }

  public static synchronized FileObject getFileObject(String vfsFilename, IVariables variables)
      throws HopFileException {
    lock.readLock().lock();
    try {
      DefaultFileSystemManager fsManager = getFileSystemManager(variables);

      try {
        // We have one problem with VFS: if the file is in a subdirectory of the current one:
        // somedir/somefile
        // In that case, VFS doesn't parse the file correctly.
        // We need to put file: in front of it to make it work.
        // However, how are we going to verify this?
        //
        // We are going to see if the filename starts with one of the known protocols like file:
        // zip: ram: smb: jar: etc.
        // If not, we are going to assume it's a file.
        //
        boolean relativeFilename = true;
        String[] initialSchemes = fsManager.getSchemes();

        vfsFilename = hop2vfsPasswordEncoder(vfsFilename, variables);

        relativeFilename = checkForScheme(initialSchemes, relativeFilename, vfsFilename);

        String filename;
        if (vfsFilename.startsWith("\\\\")) {
          File file = new File(vfsFilename);
          filename = file.toURI().toString();
        } else {
          if (relativeFilename) {
            File file = new File(vfsFilename);
            filename = file.getAbsolutePath();
          } else {
            filename = vfsFilename;
          }
        }

        return fsManager.resolveFile(filename);
      } catch (Exception e) {
        throw new HopFileException(
            "Unable to get VFS File object for filename '"
                + cleanseFilename(vfsFilename)
                + "' : "
                + e.getMessage(),
            e);
      }
    } finally {
      lock.readLock().unlock();
    }
  }

  public static synchronized FileObject getFileObject(String vfsFilename) throws HopFileException {
    lock.readLock().lock();
    try {
      DefaultFileSystemManager fsManager = getFileSystemManager();

      try {
        // We have one problem with VFS: if the file is in a subdirectory of the current one:
        // somedir/somefile
        // In that case, VFS doesn't parse the file correctly.
        // We need to put file: in front of it to make it work.
        // However, how are we going to verify this?
        //
        // We are going to see if the filename starts with one of the known protocols like file:
        // zip: ram: smb: jar: etc.
        // If not, we are going to assume it's a file.
        //
        boolean relativeFilename = true;
        String[] initialSchemes = fsManager.getSchemes();

        vfsFilename = hop2vfsPasswordEncoder(vfsFilename, null);

        relativeFilename = checkForScheme(initialSchemes, relativeFilename, vfsFilename);

        String filename;
        if (vfsFilename.startsWith("\\\\")) {
          File file = new File(vfsFilename);
          filename = file.toURI().toString();
        } else {
          if (relativeFilename) {
            File file = new File(vfsFilename);
            filename = file.getAbsolutePath();
          } else {
            filename = vfsFilename;
          }
        }

        return fsManager.resolveFile(filename);
      } catch (Exception e) {
        throw new HopFileException(
            "Unable to get VFS File object for filename '"
                + cleanseFilename(vfsFilename)
                + "' : "
                + e.getMessage(),
            e);
      }
    } finally {
      lock.readLock().unlock();
    }
  }

  protected static boolean checkForScheme(
      String[] initialSchemes, boolean relativeFilename, String vfsFilename) {
    if (vfsFilename == null) {
      return false;
    }
    for (int i = 0; i < initialSchemes.length && relativeFilename; i++) {
      if (vfsFilename.startsWith(initialSchemes[i] + ":")) {
        relativeFilename = false;
      }
    }
    return relativeFilename;
  }

  /**
   * Private method for stripping password from filename when a FileObject can not be obtained.
   * getFriendlyURI(FileObject) or getFriendlyURI(String) are the public methods.
   */
  private static String cleanseFilename(String vfsFilename) {
    return vfsFilename.replaceAll(":[^:@/]+@", ":<password>@");
  }

  /**
   * Converts Hop-encrypted passwords in VFS URIs to Commons VFS encrypted format.
   *
   * <p>Extracts the password component from a VFS URI (format: scheme://user:password@host...),
   * decrypts it using Hop's {@link Encr#decryptPasswordOptionallyEncrypted(String)}, and wraps it
   * in braces for Commons VFS compatibility.
   *
   * <p>Returns the original input unchanged if:
   *
   * <ul>
   *   <li>Input is null or blank
   *   <li>Input is not a valid URI (no scheme)
   *   <li>URI has no password segment (e.g., sftp://user@host/path)
   *   <li>Password does not change after decryption (not Hop-encrypted)
   *   <li>URI format is invalid (e.g., sftp://user:pass/path without @)
   * </ul>
   *
   * @param vfsFilename the VFS URI to process (e.g., sftp://user:Encrypted abc123@host/path)
   * @param variables the IVariables context for resolving variable references in the password
   * @return the URI with encrypted password wrapped in braces if changed, or the original input if
   *     no changes were needed
   */
  static String hop2vfsPasswordEncoder(String vfsFilename, IVariables variables) {
    // Early return for null or blank input
    if (StringUtils.isBlank(vfsFilename)) {
      return vfsFilename;
    }

    // Find scheme boundary (://) or (\\)
    int schemeEnd = vfsFilename.indexOf("://");
    if (schemeEnd == -1) {
      schemeEnd = vfsFilename.indexOf("\\\\");
      if (schemeEnd == -1) {
        // No valid scheme found, return unchanged
        return vfsFilename;
      }
      schemeEnd += 2; // Move past ":\\"
    } else {
      schemeEnd += 3; // Move past "://"
    }

    // Find first ':' (username:password delimiter) after scheme
    int colonPos = vfsFilename.indexOf(':', schemeEnd);
    if (colonPos == -1) {
      // No password delimiter found, return unchanged (e.g., sftp://user/path)
      return vfsFilename;
    }

    // Find '@' that separates credentials from host (must be before first '/' or '\')
    int atPos = vfsFilename.indexOf('@', colonPos);
    if (atPos == -1) {
      // No '@' found after ':', invalid URI format, return unchanged
      return vfsFilename;
    }

    // Verify '@' comes before the next '/' or '\' (path separator)
    int nextSlash = vfsFilename.indexOf('/', atPos);
    int nextBackslash = vfsFilename.indexOf('\\', atPos);
    if (nextSlash == -1) {
      nextSlash = Integer.MAX_VALUE;
    }
    if (nextBackslash == -1) {
      nextBackslash = Integer.MAX_VALUE;
    }

    // If '@' is not before first path separator, it's part of path, not credentials
    if (atPos > Math.min(nextSlash, nextBackslash)) {
      return vfsFilename;
    }

    // Extract password from colonPos+1 to atPos
    String password = vfsFilename.substring(colonPos + 1, atPos);

    // Resolve variables in the password
    String resolvedPassword = variables != null ? variables.resolve(password) : password;

    // Try to decrypt the password using Hop's decryption (optionally encrypted)
    // If the encoder is not available, treat the password as plain text
    String decryptedPassword;
    try {
      decryptedPassword = Encr.decryptPasswordOptionallyEncrypted(resolvedPassword);
    } catch (Exception e) {
      // Encoder not available or decryption failed, return original
      return vfsFilename;
    }

    // If decrypted password equals the resolved password, it was not Hop-encrypted
    // Return the original input unchanged
    if (decryptedPassword.equals(resolvedPassword)) {
      return vfsFilename;
    }

    // Wrap the decrypted password in braces for Commons VFS compatibility
    Cryptor cryptor = CryptorFactory.getCryptor();
    String vfsEncryptedPassword = null;
    try {
      vfsEncryptedPassword = cryptor.encrypt(decryptedPassword);
    } catch (Exception e) {
      return vfsFilename;
    }

    // Replace the original password with the braced one
    String result =
        vfsFilename.substring(0, colonPos + 1)
            + "{"
            + vfsEncryptedPassword
            + "}"
            + vfsFilename.substring(atPos);

    return result;
  }

  /**
   * Read a text file (like an XML document). WARNING DO NOT USE FOR DATA FILES.
   *
   * @param vfsFilename the filename or URL to read from
   * @param charSetName the character set of the string (UTF-8, ISO8859-1, etc)
   * @return The content of the file as a String
   * @throws org.apache.hop.core.exception.HopFileException
   */
  public static String getTextFileContent(String vfsFilename, String charSetName)
      throws HopFileException {
    try {
      InputStream inputStream = getInputStream(vfsFilename);

      InputStreamReader reader = new InputStreamReader(inputStream, charSetName);
      int c;
      StringBuilder aBuffer = new StringBuilder();
      while ((c = reader.read()) != -1) {
        aBuffer.append((char) c);
      }
      reader.close();
      inputStream.close();

      return aBuffer.toString();
    } catch (IOException e) {
      throw new HopFileException(e);
    }
  }

  public static boolean fileExists(String vfsFilename) throws HopFileException {
    FileObject fileObject = null;
    try {
      fileObject = getFileObject(vfsFilename);
      return fileObject.exists();
    } catch (IOException e) {
      throw new HopFileException(e);
    } finally {
      if (fileObject != null) {
        try {
          fileObject.close();
        } catch (Exception e) {
          /* Ignore */
        }
      }
    }
  }

  public static boolean fileExists(String vfsFilename, IVariables variables)
      throws HopFileException {
    FileObject fileObject = null;
    try {
      fileObject = getFileObject(vfsFilename, variables);
      return fileObject.exists();
    } catch (IOException e) {
      throw new HopFileException(e);
    } finally {
      if (fileObject != null) {
        try {
          fileObject.close();
        } catch (Exception e) {
          /* Ignore */
        }
      }
    }
  }

  public static InputStream getInputStream(FileObject fileObject) throws FileSystemException {
    FileContent content = fileObject.getContent();
    return content.getInputStream();
  }

  public static InputStream getInputStream(String vfsFilename) throws HopFileException {
    try {
      FileObject fileObject = getFileObject(vfsFilename);

      return getInputStream(fileObject);
    } catch (IOException e) {
      throw new HopFileException(e);
    }
  }

  public static InputStream getInputStream(String vfsFilename, IVariables variables)
      throws HopFileException {
    try {
      FileObject fileObject = getFileObject(vfsFilename, variables);

      return getInputStream(fileObject);
    } catch (IOException e) {
      throw new HopFileException(e);
    }
  }

  public static OutputStream getOutputStream(FileObject fileObject, boolean append)
      throws IOException {
    FileObject parent = fileObject.getParent();
    if (parent != null && !parent.exists()) {
      throw new IOException(
          BaseMessages.getString(
              PKG, "HopVFS.Exception.ParentDirectoryDoesNotExist", getFriendlyURI(parent)));
    }
    try {
      // Temporary work-around for VFS-807 bug (can be removed after 2.9.0)
      //
      if (fileObject.exists() && !append) {
        // Content was not removed at the trailing end of the file
        // Se we're going to delete the file first in this scenario
        //
        fileObject.delete();
      }
      fileObject.createFile();
      FileContent content = fileObject.getContent();
      return content.getOutputStream(append);
    } catch (FileSystemException e) {
      // Perhaps if it's a local file, we can retry using the standard
      // File object. This is because on Windows there is a bug in VFS.
      //
      if (fileObject instanceof LocalFile) {
        try {
          String filename = getFilename(fileObject);
          return new FileOutputStream(new File(filename), append);
        } catch (Exception e2) {
          throw e; // throw the original exception: hide the retry.
        }
      } else {
        throw e;
      }
    }
  }

  public static OutputStream getOutputStream(
      String vfsFilename, boolean append, IVariables variables) throws HopFileException {
    try {
      FileObject fileObject = getFileObject(vfsFilename, variables);
      return getOutputStream(fileObject, append);
    } catch (IOException e) {
      throw new HopFileException(e);
    }
  }

  public static OutputStream getOutputStream(String vfsFilename, boolean append)
      throws HopFileException {
    try {
      FileObject fileObject = getFileObject(vfsFilename);
      return getOutputStream(fileObject, append);
    } catch (IOException e) {
      throw new HopFileException(e);
    }
  }

  public static String getFilename(FileObject fileObject) {
    FileName fileName = fileObject.getName();
    String root = fileName.getRootURI();
    if (!root.startsWith("file:")) {
      return fileName.getURI(); // nothing we can do about non-normal files.
    }
    if (root.startsWith("file:////")) {
      return fileName.getURI(); // we'll see 4 forward slashes for a windows/smb network share
    }
    if (root.endsWith(":/")) { // Windows
      root = root.substring(8, 10);
    } else { // *nix & OSX
      root = "";
    }
    String fileString = root + fileName.getPath();
    if (!"/".equals(Const.FILE_SEPARATOR)) {
      fileString = Const.replace(fileString, "/", Const.FILE_SEPARATOR);
    }
    return fileString;
  }

  public static String getFriendlyURI(String filename) {
    if (filename == null) {
      return null;
    }
    String friendlyName;
    try {
      friendlyName = getFriendlyURI(HopVfs.getFileObject(filename));
    } catch (Exception e) {
      // unable to get a friendly name from VFS object.
      // Cleanse name of pwd before returning
      friendlyName = cleanseFilename(filename);
    }
    return friendlyName;
  }

  public static String getFriendlyURI(String filename, IVariables variables) {
    if (filename == null) {
      return null;
    }
    String friendlyName;
    try {
      friendlyName = getFriendlyURI(HopVfs.getFileObject(filename, variables));
    } catch (Exception e) {
      // unable to get a friendly name from VFS object.
      // Cleanse name of pwd before returning
      friendlyName = cleanseFilename(filename);
    }
    return friendlyName;
  }

  public static String getFriendlyURI(FileObject fileObject) {
    return fileObject.getName().getFriendlyURI();
  }

  /**
   * Creates a file using "java.io.tmpdir" directory
   *
   * @param prefix - file name
   * @param prefix - file extension
   * @return FileObject
   * @throws HopFileException
   */
  public static FileObject createTempFile(String prefix, Suffix suffix) throws HopFileException {
    return createTempFile(prefix, suffix.ext, TEMP_DIR);
  }

  /**
   * @param prefix - file name
   * @param suffix - file extension
   * @param directory - directory where file will be created
   * @return FileObject
   * @throws HopFileException
   */
  public static synchronized FileObject createTempFile(
      String prefix, String suffix, String directory) throws HopFileException {
    try {
      FileObject fileObject;
      do {
        // Temporary files are always stored locally.
        // No other schemes besides file:// make sense
        //
        String baseUrl;
        if (directory.contains("://")) {
          baseUrl = directory;
        } else {
          File directoryFile = new File(directory);
          baseUrl = "file://" + directoryFile.getAbsolutePath();
        }

        // Build temporary file name using UUID to ensure uniqueness. Old mechanism would fail using
        // Sort Rows (for example)
        // when there multiple nodes with multiple JVMs on each node. In this case, the temp file
        // names would end up being
        // duplicated which would cause the sort to fail.
        //
        String filename = baseUrl + "/" + prefix + "_" + UUID.randomUUID() + suffix;

        fileObject = getFileObject(filename);
      } while (fileObject.exists());
      return fileObject;
    } catch (IOException e) {
      throw new HopFileException(e);
    }
  }

  /**
   * @param prefix - file name
   * @param suffix - file extension
   * @param directory - directory where file will be created
   * @return FileObject
   * @throws HopFileException
   */
  public static synchronized FileObject createTempFile(
      String prefix, String suffix, String directory, IVariables variables)
      throws HopFileException {
    try {
      FileObject fileObject;
      do {
        // Temporary files are always stored locally.
        // No other schemes besides file:// make sense
        //
        String baseUrl;
        if (directory.contains("://")) {
          baseUrl = directory;
        } else {
          File directoryFile = new File(directory);
          baseUrl = "file://" + directoryFile.getAbsolutePath();
        }

        // Build temporary file name using UUID to ensure uniqueness. Old mechanism would fail using
        // Sort Rows (for example)
        // when there multiple nodes with multiple JVMs on each node. In this case, the temp file
        // names would end up being
        // duplicated which would cause the sort to fail.
        //
        String filename = baseUrl + "/" + prefix + "_" + UUID.randomUUID() + suffix;

        fileObject = getFileObject(filename, variables);
      } while (fileObject.exists());
      return fileObject;
    } catch (IOException e) {
      throw new HopFileException(e);
    }
  }

  public static Comparator<FileObject> getComparator() {
    return (o1, o2) -> {
      String filename1 = getFilename(o1);
      String filename2 = getFilename(o2);
      return filename1.compareTo(filename2);
    };
  }

  /**
   * Check if filename starts with one of the known protocols like file: zip: ram: smb: jar: etc. If
   * yes, return true otherwise return false
   *
   * @param vfsFileName
   * @return boolean
   */
  public static boolean startsWithScheme(String vfsFileName, IVariables variables) {
    lock.readLock().lock();
    try {

      DefaultFileSystemManager fsManager = getFileSystemManager(variables);

      boolean found = false;
      String[] schemes = fsManager.getSchemes();
      for (int i = 0; i < schemes.length; i++) {
        if (vfsFileName.startsWith(schemes[i] + ":")) {
          found = true;
          break;
        }
      }

      return found;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Find files with a specific extension in the specified folder and optionally
   *
   * @param folder The folder to search in
   * @param extension The extension of null if you want all files
   * @param includeSubFolders True if you want to search sub-folders as well.
   * @return The list of files found
   * @throws Exception
   */
  public static final List<FileObject> findFiles(
      FileObject folder, String extension, boolean includeSubFolders) throws Exception {
    List<FileObject> files = new ArrayList<>();

    for (FileObject child : folder.getChildren()) {
      if (child.isFolder() && includeSubFolders) {
        files.addAll(findFiles(child, extension, true));
      } else {
        if (StringUtils.isEmpty(extension)
            || extension.equalsIgnoreCase(child.getName().getExtension())) {
          files.add(child);
        }
      }
    }

    return files;
  }

  /**
   * @see StandardFileSystemManager#freeUnusedResources()
   */
  public static void freeUnusedResources() {
    if (fsm != null) {
      fsm.freeUnusedResources();
    }
  }

  public static void reset() {
    if (fsm != null) {
      fsm.freeUnusedResources();
      fsm.close();
      fsm = null;
    }
    if (extendedFsm != null) {
      extendedFsm.freeUnusedResources();
      extendedFsm.close();
      extendedFsm = null;
    }
  }

  public enum Suffix {
    ZIP(".zip"),
    TMP(".tmp"),
    JAR(".jar");

    private String ext;

    Suffix(String ext) {
      this.ext = ext;
    }
  }
}
