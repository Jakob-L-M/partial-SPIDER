package runner;

import core.Spider;

import java.io.IOException;

public class Runner {
    public static void main(String[] args) throws IOException, InterruptedException {

        Config config = new Config(1.0, Config.NullHandling.SUBSET, Config.DuplicateHandling.AWARE);

        if (args.length == 2) {
            Config.Dataset dataset = switch (args[0]) {
                case "Cars" -> Config.Dataset.CARS;
                case "ACNH" -> Config.Dataset.ANIMAL_CROSSING;
                case "T2D" -> Config.Dataset.T2D;
                case "WebTables" -> Config.Dataset.WEB_TABLES;
                case "US" -> Config.Dataset.DATA_GOV;
                case "EU" -> Config.Dataset.EU;
                case "Population" -> Config.Dataset.POPULATION;
                case "Musicbrainz" -> Config.Dataset.MUSICBRAINZ;
                case "UniProt" -> Config.Dataset.ENSEMBL_UNIPROT;
                case "Tesma" -> Config.Dataset.TESMA;
                case "TPC-H 1" -> Config.Dataset.TPCH_1;
                case "TPC-H 10" -> Config.Dataset.TPCH_10;
                default -> null;
            };
            if (dataset == null) {
                System.out.println("Unknown Dataset");
                return;
            }
            config.setDataset(dataset);

            config.parallel = Integer.parseInt(args[1]);
        } else {
            config.setDataset(Config.Dataset.TPCH_1);
            config.parallel = 10;
        }
        Spider spider = new Spider(config);
        spider.execute();
    }
}
