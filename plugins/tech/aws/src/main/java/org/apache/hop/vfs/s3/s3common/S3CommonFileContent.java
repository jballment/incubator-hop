package org.apache.hop.vfs.s3.s3common;

// S3CommonFileContent.java

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.transfer.Transfer;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.TransferProgress;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.cert.Certificate;
import java.util.Map;
import org.apache.commons.vfs2.FileContent;
import org.apache.commons.vfs2.FileContentInfo;
import org.apache.commons.vfs2.FileContentInfoFactory;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.RandomAccessContent;
import org.apache.commons.vfs2.provider.DefaultFileContent;
import org.apache.commons.vfs2.provider.local.LocalFile;
import org.apache.commons.vfs2.util.RandomAccessMode;

/**
 * A wrapper class for org.apache.commons.vfs2.provider.DefaultFileContent. This allows for easy
 * extension or interception of methods related to file content in an S3-specific VFS provider
 * context.
 */
public class S3CommonFileContent implements FileContent {

  // The underlying DefaultFileContent instance that this class wraps.
  private final DefaultFileContent wrappedContent;
  private final S3CommonFileObject s3FileObject;

  public S3CommonFileContent(
      S3CommonFileObject fileObject, FileContentInfoFactory fileContentInfoFactory) {
    this.s3FileObject = fileObject;
    wrappedContent = new DefaultFileContent(fileObject, fileContentInfoFactory);
  }

  // --- Delegation of FileContent methods to the wrapped instance ---

  /**
   * @see FileContent#getFile()
   */
  @Override
  public FileObject getFile() {
    return wrappedContent.getFile();
  }

  /**
   * @see FileContent#getInputStream()
   */
  @Override
  public InputStream getInputStream() throws FileSystemException {
    return wrappedContent.getInputStream();
  }

  /**
   * @see FileContent#getOutputStream()
   */
  @Override
  public OutputStream getOutputStream() throws FileSystemException {
    return wrappedContent.getOutputStream();
  }

  /**
   * @see FileContent#getOutputStream(boolean)
   */
  @Override
  public OutputStream getOutputStream(boolean bAppend) throws FileSystemException {
    return wrappedContent.getOutputStream(bAppend);
  }

  @Override
  public RandomAccessContent getRandomAccessContent(RandomAccessMode randomAccessMode)
      throws FileSystemException {
    return wrappedContent.getRandomAccessContent(randomAccessMode);
  }

  /**
   * @see FileContent#write(FileContent)
   */
  @Override
  public long write(FileContent fileContent) throws FileSystemException {
    try {
      return wrappedContent.write(fileContent);
    } catch (IOException e) {
      throw new FileSystemException(e);
    }
  }

  /**
   * @see FileContent#write(FileObject)
   */
  @Override
  public long write(FileObject fileObject) throws FileSystemException {
    try {
      if (fileObject instanceof org.apache.commons.vfs2.provider.local.LocalFile) {
        try (LocalFile localFile = (LocalFile) fileObject) {
          try (S3CommonFileSystem s3fs = (S3CommonFileSystem) this.getFile().getFileSystem()) {
            AmazonS3 client = s3fs.getS3Client();
            TransferManager transferManager =
                TransferManagerBuilder.standard().withS3Client(client).build();
            File dstFile = new File(localFile.getURI());
            Transfer transfer =
                transferManager.download(s3FileObject.bucketName, s3FileObject.key, dstFile);
            transfer.waitForCompletion();
            TransferProgress transferProgress = transfer.getProgress();
            long transferredBytes = transferProgress.getBytesTransferred();
            transferManager.shutdownNow();
            return transferredBytes;
          }
        } catch (Exception e) {
          return wrappedContent.write(fileObject);
        }
      } else {
        return wrappedContent.write(fileObject);
      }
    } catch (IOException e) {
      throw new FileSystemException(e);
    }
  }

  @Override
  public long write(OutputStream outputStream) throws IOException {
    return wrappedContent.write(outputStream);
  }

  @Override
  public long write(OutputStream outputStream, int i) throws IOException {
    return wrappedContent.write(outputStream, i);
  }

  /**
   * @see FileContent#getCertificates()
   */
  @Override
  public Certificate[] getCertificates() throws FileSystemException {
    return wrappedContent.getCertificates();
  }

  /**
   * @see FileContent#getLastModifiedTime()
   */
  @Override
  public long getLastModifiedTime() throws FileSystemException {
    return wrappedContent.getLastModifiedTime();
  }

  /**
   * @see FileContent#getSize()
   */
  @Override
  public long getSize() throws FileSystemException {
    return wrappedContent.getSize();
  }

  @Override
  public boolean hasAttribute(String s) throws FileSystemException {
    return wrappedContent.hasAttribute(s);
  }

  /**
   * @see FileContent#close()
   */
  @Override
  public void close() throws FileSystemException {
    wrappedContent.close();
  }

  /**
   * @see FileContent#isOpen()
   */
  @Override
  public boolean isOpen() {
    return wrappedContent.isOpen();
  }

  @Override
  public void removeAttribute(String s) throws FileSystemException {
    wrappedContent.removeAttribute(s);
  }

  @Override
  public void setAttribute(String s, Object o) throws FileSystemException {
    wrappedContent.setAttribute(s, o);
  }

  @Override
  public void setLastModifiedTime(long l) throws FileSystemException {
    wrappedContent.setLastModifiedTime(l);
  }

  /**
   * @see FileContent#getContentInfo()
   */
  @Override
  public FileContentInfo getContentInfo() throws FileSystemException {
    return wrappedContent.getContentInfo();
  }

  /**
   * @see FileContent#getAttributes()
   */
  @Override
  public Map<String, Object> getAttributes() throws FileSystemException {
    return wrappedContent.getAttributes();
  }

  /**
   * @see FileContent#getAttribute(String)
   */
  @Override
  public Object getAttribute(String attrName) throws FileSystemException {
    return wrappedContent.getAttribute(attrName);
  }

  @Override
  public String[] getAttributeNames() throws FileSystemException {
    return wrappedContent.getAttributeNames();
  }
}
