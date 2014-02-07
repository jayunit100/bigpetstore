package org.bigtop.bigpetstore.etl;

import java.util.Map;

import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pipeline;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.bigtop.bigpetstore.contract.PetStoreStatistics;

public class CrunchETL extends PetStoreStatistics {

    // STATIC function declarations, so that we dont run into serialization
    // reference issues.
    // See docs for DoFn for reasons why...
    public static MapFn<LineItem, String> COUNT_BY_PRODUCT = new MapFn<LineItem, String>() {
        public String map(LineItem lineItem) {
            try {
                return lineItem.getDescription();
            } catch (Throwable t) {
                throw new RuntimeException(t);
            }
        }
    };
    public static MapFn<LineItem, String> COUNT_BY_STATE = new MapFn<LineItem, String>() {
        public String map(LineItem lineItem) {
            try {
                return lineItem.getDescription();
            } catch (Throwable t) {
                throw new RuntimeException(t);
            }
        }
    };

    PCollection<LineItem> lineItems;

    public CrunchETL(Path input, Path output) throws Exception {
        Pipeline pipeline = new MRPipeline(CrunchETL.class);
        PCollection<String> lines = pipeline.read(From.textFile(new Path(input,
                "part-r-00000")));
        System.out.println("crunch : " + lines.getName() + "  "
                + lines.getSize());
        // System.out.println(FileSystem.get(new
        // Configuration()).getLength(input));

        lineItems = lines.parallelDo(ETL, Avros.reflects(LineItem.class));

    }

    public static MapFn ETL = new MapFn<String, LineItem>() {
        @Override
        public LineItem map(String input) {
            String[] fields = input.split(",");
            LineItem li = new LineItem();
            li.setAppName(fields[1]);
            li.setFirstName(fields[3]);
            // ...
            li.setDescription(fields[fields.length - 1]);
            return li;
        }
    };

    @Override
    public Map<String, ? extends Number> numberOfTransactionsByState()
            throws Exception {
        PTable<String, Long> counts = lineItems.parallelDo(COUNT_BY_STATE,
                Avros.strings()).count();
        Map m = counts.materializeToMap();
        System.out.println("Crunch:::  " + m);
        return m;
    }

    @Override
    public Map<String, ? extends Number> numberOfProductsByProduct()
            throws Exception {
        PTable<String, Long> counts = lineItems.parallelDo(COUNT_BY_PRODUCT,
                Avros.strings()).count();
        Map m = counts.materializeToMap();
        System.out.println("Crunch:::  " + m);
        return m;
    }

    /**
     * This is just prototype code, we will modify it later, but it works.
     * 
     * @param args
     * @throws Exception
     */
    public static void main(String... args) throws Exception {
        /**
         * PCollection<String> lines = MemPipeline .collectionOf(
         * "BigPetStore,storeCode_AK,1  lindsay,franco,Sat Jan 10 00:11:10 EST 1970,10.5,dog-food"
         * ,
         * "BigPetStore,storeCode_AZ,1  tom,giles,Sun Dec 28 23:08:45 EST 1969,10.5,dog-food"
         * ,
         * "BigPetStore,storeCode_CA,1  brandon,ewing,Mon Dec 08 20:23:57 EST 1969,16.5,organic-dog-food"
         * ,
         * "BigPetStore,storeCode_CA,2  angie,coleman,Thu Dec 11 07:00:31 EST 1969,10.5,dog-food"
         * ,
         * "BigPetStore,storeCode_CA,3  angie,coleman,Tue Jan 20 06:24:23 EST 1970,7.5,cat-food"
         * ,
         * "BigPetStore,storeCode_CO,1  sharon,trevino,Mon Jan 12 07:52:10 EST 1970,30.1,antelope snacks"
         * ,
         * "BigPetStore,storeCode_CT,1  kevin,fitzpatrick,Wed Dec 10 05:24:13 EST 1969,10.5,dog-food"
         * ,
         * "BigPetStore,storeCode_NY,1  dale,holden,Mon Jan 12 23:02:13 EST 1970,19.75,fish-food"
         * ,
         * "BigPetStore,storeCode_NY,2  dale,holden,Tue Dec 30 12:29:52 EST 1969,10.5,dog-food"
         * ,
         * "BigPetStore,storeCode_OK,1  donnie,tucker,Sun Jan 18 04:50:26 EST 1970,7.5,cat-food"
         * );
         **/
        // FAILS
        Pipeline pipeline = new MRPipeline(CrunchETL.class);

        PCollection<String> lines = pipeline.read(From.textFile(new Path(
                "/tmp/BigPetStore1388719888255/generated/part-r-00000")));
        PCollection<LineItem> lineItems = lines.parallelDo(
                new MapFn<String, LineItem>() {
                    @Override
                    public LineItem map(String input) {

                        System.out.println("proc1 " + input);
                        String[] fields = input.split(",");
                        LineItem li = new LineItem();
                        li.setAppName("" + fields[1]);
                        li.setFirstName("" + fields[3]);
                        li.setDescription("" + fields[fields.length - 1]);
                        return li;
                    }
                }, Avros.reflects(LineItem.class));
        /**
         * //System.out.println(); final PTable<String, Long> counts =
         * lineItems.parallelDo( new MapFn<LineItem, String>() { public String
         * map(LineItem lineItem) { try{ System.out.println("proc2"); return
         * lineItem.getAppName(); } catch(Throwable t){ throw new
         * RuntimeException(t); } } }, Avros.strings()).count();
         * 
         * Map m = counts.materializeToMap(); System.out.println(m);
         */

        for (LineItem i : lineItems.materialize())
            System.out.println(i);

        // System.out.println(lineItems.getSize() + " " + lineItems.count() +
        // " " + lineItems.asCollection().getValue().size());
    }

}