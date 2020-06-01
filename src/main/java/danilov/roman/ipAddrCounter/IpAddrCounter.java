package danilov.roman.ipAddrCounter;

import java.io.*;

public class IpAddrCounter {

    /**
     * Подсчет количества уникальных строк в файле большог размера.
     * Берется файл, делится на файлы меньшего размера.
     * Из этих файлов данные сортируются и удаляются дубликаты.
     * Дальше происходит слияние всех мелких файлов в более большие, с удалением дубликатов.
     * И так пока не получится один большой файл без дубликатов.
     *
     */
    public static void main(String[] args) throws Exception {
        File file = new File(IpAddrCounter.class.getClassLoader().getResource("ip-list.txt").getFile());
        PartitionBigFile partitionBigFile = new PartitionBigFile(file);
        partitionBigFile.start();
        MergeFilesData mergeFilesData = new MergeFilesData();
        mergeFilesData.start();
    }
}
