package danilov.roman.ipAddrCounter;

import java.io.*;
import java.nio.file.NotDirectoryException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.*;

public class MergeFilesData {

    private int numFiles = 0;
    private final String fileDir = "splits";
    private File[] fileList;

    public void start() {
        try {
            createFileList();
            mergeFiles(fileList);
        } catch (Exception ex) {
            System.err.println(ex.getMessage());
        }
    }

    /**
     * Получение файлов из дериктории
     * @throws IOException
     */
    private void createFileList() throws IOException {
        File folder = new File(fileDir);
        fileList = folder.listFiles();
        if (fileList != null) {
            numFiles = fileList.length;
            System.out.println("No. of files - " + numFiles);
        } else {
            throw new NotDirectoryException("NO DIRECTORY");
        }
    }

    /**
     * Метод для объединения файлов в один.
     *
     * @param fileList Список файлов, которые нужно объеденить.
     */
    private void mergeFiles(File[] fileList) {
        Arrays.stream(fileList).parallel().reduce(this::mergeData);

//
//        int fileListLength = fileList.length;
//        if (fileListLength == 1) {
//            return fileList;
//        }
//        int sizeNewMergeFiles = fileListLength / 2;
//        if (fileListLength != 2) {
//            sizeNewMergeFiles = (fileListLength % 2 == 0) ? sizeNewMergeFiles : sizeNewMergeFiles + 1;
//        }
//
//        //CountDownLatch countDownLatch = new CountDownLatch(sizeNewMergeFiles);
//
//        File[] newMergeFiles = new File[sizeNewMergeFiles];
//        int k = 0;
//        for (int i = 0; i < fileListLength; i += 2) {
//            if (i + 1 >= fileListLength) {
//                newMergeFiles[k] = fileList[i];
//            } else {
//                newMergeFiles[k++] = mergeData(fileList[i], fileList[i + 1]);
//            }
//        }
//        return mergeFiles(newMergeFiles);
    }

    private File mergeData(File file1, File file2) {
        System.out.println(Thread.currentThread().getName());
        String mergeFileName = Paths.get(fileDir, UUID.randomUUID().toString()).toString();
        try (PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(mergeFileName, true)))) {
            if (file1.length() >= file2.length()) {
                writeToFile(file1, file2, pw);
            } else {
                writeToFile(file2, file1, pw);
            }
            pw.flush();
            file1.deleteOnExit();
            file2.deleteOnExit();
        } catch (IOException ex) {
            System.err.println(ex.toString());
        }
        return new File(mergeFileName);
    }

    /**
     * Объединение данных из файлов методом слияния.
     * Так же удаляются дубликаты.
     *
     * @param file1 File Первый файл
     * @param file2 File Второй файл
     * @param writer PrintWriter
     * @throws IOException
     */
    private void writeToFile(File file1, File file2, PrintWriter writer)
            throws IOException {
        BufferedReader reader1 = new BufferedReader(new FileReader(file1));
        BufferedReader reader2 = new BufferedReader(new FileReader(file2));
        String line1 = reader1.readLine();
        String line2 = reader2.readLine();
        long countElements = 0L;
        while (line1 != null) {
            if (line2 != null) {
                int compareLine1Line2 = line1.compareTo(line2);
                if (compareLine1Line2 < 0) {
                    writer.println(line1);
                    line1 = reader1.readLine();
                    countElements++;
                } else if (compareLine1Line2 > 0) {
                    writer.println(line2);
                    line2 = reader2.readLine();
                    countElements++;
                } else {
                    // убираем дубликаты
                    writer.println(line1);
                    line1 = reader1.readLine();
                    line2 = reader2.readLine();
                    countElements++;
                }
            } else {
                writer.println(line1);
                line1 = reader1.readLine();
                countElements++;
            }
        }
        if (line2 != null) {
            while (line2 != null) {
                writer.println(line2);
                line2 = reader2.readLine();
                countElements++;
            }
        }
        System.out.println("Количество уникальных элементов в файле: " + countElements);
        reader1.close();
        reader2.close();
    }
}
