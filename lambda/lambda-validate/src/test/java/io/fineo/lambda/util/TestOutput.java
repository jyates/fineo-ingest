package io.fineo.lambda.util;

import com.google.common.base.Preconditions;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

/**
 * Derivative of {@link org.junit.rules.TemporaryFolder}, but doesn't delete the output
 */
public class TestOutput extends TemporaryFolder {

  private final boolean shouldDelete;

  public TestOutput(boolean shouldDelete) {
    this.shouldDelete = shouldDelete;
  }

  @Override
  protected void after() {
    if (shouldDelete) {
      delete();
    }
  }

//  public File getNeworExistingFolder(String ... names) throws IOException {
//    try{
//      return super.newFolder(names);
//    }catch (IOException e){
//      // check to see if all the names were created
//      File file = getRoot();
//      for (int i = 0; i < names.length; i++) {
//        String folderName = names[i];
//        file = new File(file, folderName);
//        // check to see why it doesn't exist
//        if(!file.exists()){
//          // might be misformatted, so we give up
//          validateFolderName(folderName);
//        }
//
//        //
//
//        if (!file.mkdir() && isLastElementInArray(i, folderNames)) {
//          throw new IOException(
//            "a folder with the name \'" + folderName + "\' already exists");
//        }
//      }
//    }
//  }

//  /**
//   * Validates if multiple path components were used while creating a folder.
//   *
//   * @param folderName
//   *            Name of the folder being created
//   */
//  private void validateFolderName(String folderName) throws IOException {
//    File tempFile = new File(folderName);
//    if (tempFile.getParent() != null) {
//      String errorMsg = "Folder name cannot consist of multiple path components separated by a
// file separator."
//                        + " Please use newFolder('MyParentFolder','MyFolder') to create
// hierarchies of folders";
//      throw new IOException(errorMsg);
//    }
//  }
//
//  private boolean isLastElementInArray(int index, String[] array) {
//    return index == array.length - 1;
//  }
}