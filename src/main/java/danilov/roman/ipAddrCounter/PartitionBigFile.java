package danilov.roman.ipAddrCounter;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class PartitionBigFile {
    private File bigFile;
    private long numSplits = 10; // на сколько делить частей
    private final int maxFileSize = 5242880; // 5 MB
    private final int maxReadBufferSize = 2000; // KB
    private final String pathForChunks = "splits";
    private final ExecutorService executor;

    public PartitionBigFile(File file) {
        executor = Executors.newCachedThreadPool();
        this.bigFile = file;
    }

    /**
     * Запуск разделения большого файла
     */
    public void start() {
        partitionFileToSmallerChunks(bigFile);
        executor.shutdown();
    }

    /**
     * Метод разделяет файл на равные файлы меньших размеров.
     * Затем после каждого создание такого файла,
     * запускается процесс сортировки и удаления дубликатов в другом потоке.
     *
     * @param file File
     */
    private void partitionFileToSmallerChunks(File file) {
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            long sourceSize = raf.length();
            //numSplits = sourceSize / maxFileSize;
            long bytesPerSplit = sourceSize/numSplits;
            long remainingBytes = sourceSize % numSplits;

            for (int split = 1; split <= numSplits; split++) {
                String fileName = "split." + split;
                remainingBytes += splitToFile(fileName, bytesPerSplit, raf, false);
                executor.execute(() -> sortDataFromFile(fileName));
            }
            if (remainingBytes > 0) {
                String fileName = "split." + (numSplits + 1);
                splitToFile(fileName, remainingBytes, raf, true);
                executor.execute(() -> sortDataFromFile(fileName));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Метод запускает буферфное чтение из файла и запись в новый файл
     * @param fileName String Новый файл, куда попадают данные
     * @param bytesPerSplit long Количество байт, которое должно содержаться в новом файле
     * @param raf RandomAccessFile Файл из которого идет чтение
     * @param lastPiece boolean Указывает, что это последнее чтение из файла
     * @return int
     */
    private int splitToFile(String fileName, long bytesPerSplit, RandomAccessFile raf, boolean lastPiece) {
        int remainingBytes = 0;
        try (BufferedOutputStream bw = new BufferedOutputStream(new FileOutputStream(getFileWithPath(fileName).toString()))) {
            if (bytesPerSplit > maxReadBufferSize) {
                long numReads = bytesPerSplit/maxReadBufferSize;
                long numRemainingRead = bytesPerSplit % maxReadBufferSize;
                for (int i = 0; i < numReads; i++) {
                    numRemainingRead += readWrite(raf, bw, maxReadBufferSize, false);
                }
                if (numRemainingRead > 0) {
                    remainingBytes += readWrite(raf, bw, numRemainingRead, lastPiece);
                }
            } else {
                remainingBytes += readWrite(raf, bw, bytesPerSplit, lastPiece);
            }
        } catch (Exception ex) {
            ex.getStackTrace();
        }
        return remainingBytes;
    }

    /**
     * Метод возвращает путь до файла, куда будут сохранятся данные
     *
     * @param fileName String Имя файла
     * @return Path
     */
    private Path getFileWithPath(String fileName) {
        return Paths.get(pathForChunks, fileName);
    }

    /**
     * Метод сортировки данных в новом созданном файле.
     * Так же удаляются дубликаты.
     * После сортировки и удаления дубликатов, данные перезаписываются в этот же файл, вместо предыдущих.
     *
     * @param fileName String
     */
    private void sortDataFromFile(String fileName) {
        System.out.println(fileName + " --- " + Thread.currentThread().getName());
        String[] arr = null;
        try (BufferedReader br = Files.newBufferedReader(getFileWithPath(fileName))) {
//            Set<String> allItems = new HashSet<>();
//            Set<String> duplicates = br.lines()
//                    .filter(n -> !allItems.add(n)) //Set.add() returns false if the item was already in the set.
//                    .collect(Collectors.toSet());
            arr = br.lines().distinct().toArray(String[]::new);
            mergeSort(arr, arr.length);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try (BufferedOutputStream bw = new BufferedOutputStream(new FileOutputStream(getFileWithPath(fileName).toString()))) {
            if (arr != null) {
                for (String s : arr) {
                    bw.write(s.getBytes());
                    bw.write('\n');
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Сортировка слиянием
     * @param a String[] Массив данных для сортировки
     * @param n int Количество элементов в массиве
     */
    public void mergeSort(String[] a, int n) {
        if (n < 2) {
            return;
        }
        int mid = n / 2;
        String[] l = Arrays.copyOfRange(a, 0, mid);
        String[] r = Arrays.copyOfRange(a, mid, n);
        mergeSort(l, mid);
        mergeSort(r, n - mid);
        merge(a, l, r, mid, n - mid);
    }

    public void merge(String[] a, String[] l, String[] r, int left, int right) {
        int i = 0, j = 0, k = 0;
        while (i < left && j < right) {
            if (l[i].compareTo(r[j]) <= 0) {
                a[k++] = l[i++];
            } else {
                a[k++] = r[j++];
            }
        }
        while (i < left) {
            a[k++] = l[i++];
        }
        while (j < right) {
            a[k++] = r[j++];
        }
    }

    /**
     * Метод для чтения байтов из первоначального файла и запись их в мелкий файл.
     * В методе читается количество байт из файла и ложится в массив.
     * С параметром finalBytes = true - идет проверка, заканчивается ли этот массив символом конца строки.
     * Если нет - бежим с конца массива и ищем перенос строки, заменяя элементы пустыми значениями.
     * Как только нашли, записываем массив байтов в мелкий файл и возвращаем указатель RandomAccessFile на количество
     * символов, которое пришлось пройти с конца массива до символа конца строки.
     *
     * @param raf RandomAccessFile
     * @param bw BufferedOutputStream
     * @param numBytes numBytes - количество байтов, которое читаем из файла
     * @param finalBytesAll finalBytes Если true - не проверяем, заканчивается ли массив с байтами переносом строки.
     * @return int - возвращаем количество байт, на которое пришлось вернутся, при проверке массива на символ конца строки.
     * @throws IOException
     */
    public int readWrite(RandomAccessFile raf, BufferedOutputStream bw, long numBytes, boolean finalBytesAll)
            throws IOException {
        byte[] buf = new byte[(int) numBytes];
        int val = raf.read(buf);
        int countBackBytes = 0;
        if (val != -1) {
            if (!finalBytesAll) {
                for (int i = buf.length - 1; i >= 0; i--) {
                    if (buf[i] != '\n') {
                        buf[i] = 0;
                        countBackBytes++;
                    } else {
                        raf.seek(raf.getFilePointer() - countBackBytes);
                        break;
                    }
                }
            }
            //bw.write(buf);
            bw.write(Arrays.copyOf(buf, buf.length - countBackBytes));
        }
        return countBackBytes;
    }
}
