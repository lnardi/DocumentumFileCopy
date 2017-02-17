/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package vale;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.io.BufferedWriter;
import java.util.logging.FileHandler;
import java.util.logging.Handler;

/**
 *
 * @author lnard
 */
public class DocumentumFileCopy {

    private static final Logger LOGGER = Logger.getLogger(DocumentumFileCopy.class.getName());
    private static PropertieFileUseful processProperties;
    private BufferedWriter csvOutputError;
    private String lineHeader;
    private long startTime = 0;

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        DocumentumFileCopy docFC = new DocumentumFileCopy();
        docFC.processFiles();

    }

    public void processFiles() {
        try {
//Creating consoleHandler and fileHandler
            Handler fileHandler = null;
            fileHandler = new FileHandler("./documentumFileCopy_%u.log", 10000000, 3, false);

            //Assigning handlers to LOGGER object
            LOGGER.setUseParentHandlers(false);
            LOGGER.addHandler(fileHandler);
            //Setting levels to handlers and LOGGER
            fileHandler.setLevel(Level.ALL);
            LOGGER.setLevel(Level.ALL);

            processProperties = new PropertieFileUseful("config.properties");
            startTime = System.currentTimeMillis();

            System.out.println("Iniciando");
            System.out.println("carregando CSVs");
            File[] csvs = finder("./");
            if (csvs != null && csvs.length > 0) {
                for (File csv : csvs) {
                    System.out.println("processando primeiro CSV");
                    readCSV(csv);
                    System.out.println("Movendo CSV: " + csv.getName());
                    moveFile(csv, PropertieFileUseful.getProp("processedPath"));//TODO - PODER RETORNAR FALHA
                }

            } else {
                System.out.println("Não foram encontrados arquivos para processamento");
            }
            System.out.println("verificando se existem outros csvs");
        } catch (Exception ex) {
            LOGGER.log(Level.SEVERE, null, ex);
        }
    }

    public File[] finder(String dirName) {
        File dir = new File(dirName);

        return dir.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String filename) {
                return filename.endsWith(".csv");
            }
        });
    }

    private void readCSV(File csv) throws IOException, Exception {

        BufferedReader csvInput = null;
        BufferedWriter csvOutput = null;
        String line = "";
        String cvsSplitBy = ";";
        int serverPathPosition = -1;
        int robjectidPosition = -1;
        int revisionField = -1;
        String serverPath = "server_path";
        String r_object_ID = "r_object_id";
        String revision = "revisao";
        long totalCopiedSize = 0;
        long maxSize = Long.parseLong(PropertieFileUseful.getProp("maxSize"));
        int fileNumber = 1;
        int processedFiles = 0;

        try {
            csvInput = new BufferedReader(new FileReader(csv));
            lineHeader = csvInput.readLine();//Pega o cabeçalho do arquivo//Armazena para utilizar no error                        
            csvOutput = new BufferedWriter(new FileWriter(processProperties.getProp("outputPath") + "/" + csv.getName()));
            csvOutput.write(lineHeader + ";output_FileName");
            csvOutput.newLine();
            String[] header = lineHeader.split(cvsSplitBy);
            //Seta o caminho onde os arquivos serão copiados
            String outputPath = PropertieFileUseful.getProp("outputPath") + csv.getName() + "_" + fileNumber + "/";
            String outputPathProcessing = PropertieFileUseful.getProp("outputPath") + csv.getName() + "_" + fileNumber + "_Processing/";

            makeDir(outputPathProcessing);

            //cria diretório a cada x arquivos.
            for (int i = 0; i < header.length; i++) {

                String column = header[i];
                if (serverPath.equals(column)) {
                    serverPathPosition = i;
                }
                if (r_object_ID.equals(column)) {
                    robjectidPosition = i;
                }
                if (revision.equals(column)) {
                    revisionField = i;
                }
            }

            if (serverPathPosition == -1 || robjectidPosition == -1 || revisionField == -1) {
                String msg = "Não encontou a coluna r_object_id ou server_path no arquivo " + csv.getName();
                System.out.println(msg);
                throw new Exception(msg);
            }

            while ((line = csvInput.readLine()) != null) {
                //Cria um novo folder quando o total de arquivos copiado atinge um tamanho determinado.                
                if (totalCopiedSize >= maxSize) {
                    totalCopiedSize = 0;
                    //Renomear antigos
                    if (!renameDir(outputPathProcessing, outputPath)) {
                        System.out.println("Falha de processamento ao renomear folder de saída");
                        //TODO - ERRO DE PROCESSAMENTO, REPORTAR UMA EXCEPTION
                    }
                    //criar novos
                    outputPathProcessing = PropertieFileUseful.getProp("outputPath") + csv.getName() + "_" + ++fileNumber + "_Processing/";
                    outputPath = PropertieFileUseful.getProp("outputPath") + csv.getName() + "_" + ++fileNumber + "/";

                    makeDir(outputPathProcessing);
                }
                // use comma as separator
                String[] columns = line.split(cvsSplitBy);
                String path = columns[serverPathPosition];
                String robjectid = columns[robjectidPosition];
                //get extension
                String[] pathSplit = path.split("\\.");
                String extension = pathSplit.length > 1 ? "." + pathSplit[pathSplit.length - 1] : "";
                String outputFileName = robjectid + extension;

                //Copia o arquivo
                File fileInput = new File(path);
                if (fileInput.exists()) {
                    try {
                        copyFileUsingStream(fileInput, new File(outputPathProcessing + outputFileName));
                    } catch (IOException ex) {
                        //Falha no processamento da cópia
                        writeErrorFile(line, csv.getName());
                        LOGGER.log(Level.SEVERE, line, ex);
                    }
                    //
                    totalCopiedSize += fileInput.length();//Registra o tamanho do arqivo;
                    csvOutput.write(line + ";" + outputFileName);
                    csvOutput.newLine();
                } else {//Arquivo de entrada não existe no caminho especificado.
                    if (revisionField >= 0 && !columns[revisionField].equals("-1")) { // e não é uma ficha
                        writeErrorFile(line, csv.getName());
                        System.out.println("Arquivo Inexistente : " + fileInput.getName());
                        LOGGER.log(Level.SEVERE, line, "Arquivo Inexistente");
                    } else {//Se é ficha pula o processamento.
                        csvOutput.write(line + ";" + outputFileName);                        
                        csvOutput.newLine();
                    }
                }
                processedFiles++;
                //Exibe o tempo de processamento
                if (processedFiles < 100 ? processedFiles % 10 == 0 : processedFiles % 100 == 0) {
                    System.out.println("Processando arquivo número " + processedFiles + "  - Tempo de Processamento: " + ((System.currentTimeMillis() - startTime) / 1000 / 60) + " Minutos");
                }
            }
            if (!renameDir(outputPathProcessing, outputPath)) {
                System.out.println("Falha de processamento ao renomear arquivos");
            }

        } finally {
            if (csvInput != null) {
                csvInput.close();
            }
            if (csvOutput != null) {
                csvOutput.flush();
                csvOutput.close();
            }
            if (csvOutputError != null) {
                csvOutputError.flush();
                csvOutputError.close();
            }
        }

    }

    private static void copyFileUsingStream(File source, File dest) throws IOException {
        InputStream is = null;
        OutputStream os = null;
        try {
            is = new FileInputStream(source);
            os = new FileOutputStream(dest);
            byte[] buffer = new byte[1024];
            int length;
            while ((length = is.read(buffer)) > 0) {
                os.write(buffer, 0, length);
            }
        } finally {
            is.close();
            os.close();
        }
    }

    private boolean moveFile(File p_file, String p_dir) {
        File file = p_file;
// Destination directory
        File dir = new File(p_dir);
// Move file to new directory
        return file.renameTo(new File(dir, file.getName()));
    }

    private void makeDir(String outputPath) {
        File fileOutputPath = new File(outputPath);
        if (!fileOutputPath.exists()) {
            fileOutputPath.mkdir();
        }

    }

    private boolean renameDir(String outputPathProcessing, String outputPath) {
        // File (or directory) with old name
        File oldFile = new File(outputPathProcessing);
        // File (or directory) with new name
        File newFile = new File(outputPath);
        // Rename file (or directory)
        return oldFile.renameTo(newFile);
    }

    private void writeErrorFile(String line, String name) throws IOException {
        if (csvOutputError == null) {
            csvOutputError = new BufferedWriter(new FileWriter(PropertieFileUseful.getProp("outputPath") + "/" + "ERROR_" + name));
            csvOutputError.write(lineHeader);
            csvOutputError.newLine();

        }
        csvOutputError.write(line);
        csvOutputError.newLine();

    }

}
