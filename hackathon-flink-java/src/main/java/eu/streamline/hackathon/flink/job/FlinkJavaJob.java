package eu.streamline.hackathon.flink.job;

import eu.streamline.hackathon.common.data.GDELTEvent;
import eu.streamline.hackathon.flink.operations.GDELTInputFormat;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Iterator;
import java.util.Objects;

public class FlinkJavaJob {


	private static final Logger LOG = LoggerFactory.getLogger(FlinkJavaJob.class);

	public static void main(String[] args) {

		ParameterTool params = ParameterTool.fromArgs(args);
		final String pathToGDELT = params.get("path");
		final String country = params.get("country", "USA");

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		DataStream<GDELTEvent> source = env
				.readFile(new GDELTInputFormat(new Path(pathToGDELT)), pathToGDELT).setParallelism(1);



        source.filter(new FilterFunction<GDELTEvent>() {
            @Override
            public boolean filter(GDELTEvent gdeltEvent) throws Exception {
                return gdeltEvent.actor1Code_countryCode != null;
            }
        }).assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<GDELTEvent>(Time.seconds(0)) {
                    @Override
                    public long extractTimestamp(GDELTEvent element) {
                        return element.day.getTime();
                    }
                }).keyBy(new KeySelector<GDELTEvent, String>() {
            @Override
            public String getKey(GDELTEvent gdeltEvent) throws Exception {
                return getContinent(gdeltEvent.actor1Code_countryCode);
            }
        }).window(TumblingEventTimeWindows.of(Time.days(1))).fold(new Tuple2<>(0.0,0),
                new FoldFunction<GDELTEvent, Tuple2<Double, Integer>>() {
                    @Override
                    public Tuple2<Double, Integer> fold(Tuple2<Double, Integer> acc, GDELTEvent o) throws Exception {
                        return new Tuple2<>(((double) acc.getField(0)) + o.avgTone, ((int) acc.getField(1)) + 1);
                    }
                },
                new WindowFunction<Tuple2<Double, Integer>, Tuple4<String, Double, Date, Date>, String, TimeWindow>() {
                    @Override
                    public void apply(String key, TimeWindow window, Iterable<Tuple2<Double,Integer>> input, Collector<Tuple4<String, Double, Date, Date>> out) throws Exception {
                        Iterator<Tuple2<Double,Integer>> it = input.iterator();
                        Tuple2<Double,Integer> tp = it.next();
                        out.collect(new Tuple4<>(key, ((double)tp.getField(0))/((int)tp.getField(1)), new Date(window.getStart()), new Date(window.getEnd())));
                    }
                }).print();  //.writeAsCsv("output.csv").setParallelism(1);

		try {
			env.execute("Flink Java GDELT Analyzer");
		} catch (Exception e) {
			LOG.error("Failed to execute Flink job {}", e);
		}

	}

	public static String getContinent(String countryCode){
		switch(countryCode) {
            case "AFG":
                return "Asia";
            case "ALA":
                return "Europe";
            case "ALB":
                return "Europe";
            case "DZA":
                return "Africa";
            case "ASM":
                return "Oceania";
            case "AND":
                return "Europe";
            case "AGO":
                return "Africa";
            case "AIA":
                return "Americas";
            case "ATA":
                return "";
            case "ATG":
                return "Americas";
            case "ARG":
                return "Americas";
            case "ARM":
                return "Asia";
            case "ABW":
                return "Americas";
            case "AUS":
                return "Oceania";
            case "AUT":
                return "Europe";
            case "AZE":
                return "Asia";
            case "BHS":
                return "Americas";
            case "BHR":
                return "Asia";
            case "BGD":
                return "Asia";
            case "BRB":
                return "Americas";
            case "BLR":
                return "Europe";
            case "BEL":
                return "Europe";
            case "BLZ":
                return "Americas";
            case "BEN":
                return "Africa";
            case "BMU":
                return "Americas";
            case "BTN":
                return "Asia";
            case "BOL":
                return "Americas";
            case "BES":
                return "Americas";
            case "BIH":
                return "Europe";
            case "BWA":
                return "Africa";
            case "BVT":
                return "";
            case "BRA":
                return "Americas";
            case "IOT":
                return "";
            case "BRN":
                return "Asia";
            case "BGR":
                return "Europe";
            case "BFA":
                return "Africa";
            case "BDI":
                return "Africa";
            case "KHM":
                return "Asia";
            case "CMR":
                return "Africa";
            case "CAN":
                return "Americas";
            case "CPV":
                return "Africa";
            case "CYM":
                return "Americas";
            case "CAF":
                return "Africa";
            case "TCD":
                return "Africa";
            case "CHL":
                return "Americas";
            case "CHN":
                return "Asia";
            case "CXR":
                return "";
            case "CCK":
                return "";
            case "COL":
                return "Americas";
            case "COM":
                return "Africa";
            case "COG":
                return "Africa";
            case "COD":
                return "Africa";
            case "COK":
                return "Oceania";
            case "CRI":
                return "Americas";
            case "CIV":
                return "Africa";
            case "HRV":
                return "Europe";
            case "CUB":
                return "Americas";
            case "CUW":
                return "Americas";
            case "CYP":
                return "Asia";
            case "CZE":
                return "Europe";
            case "DNK":
                return "Europe";
            case "DJI":
                return "Africa";
            case "DMA":
                return "Americas";
            case "DOM":
                return "Americas";
            case "ECU":
                return "Americas";
            case "EGY":
                return "Africa";
            case "SLV":
                return "Americas";
            case "GNQ":
                return "Africa";
            case "ERI":
                return "Africa";
            case "EST":
                return "Europe";
            case "ETH":
                return "Africa";
            case "FLK":
                return "Americas";
            case "FRO":
                return "Europe";
            case "FJI":
                return "Oceania";
            case "FIN":
                return "Europe";
            case "FRA":
                return "Europe";
            case "GUF":
                return "Americas";
            case "PYF":
                return "Oceania";
            case "ATF":
                return "";
            case "GAB":
                return "Africa";
            case "GMB":
                return "Africa";
            case "GEO":
                return "Asia";
            case "DEU":
                return "Europe";
            case "GHA":
                return "Africa";
            case "GIB":
                return "Europe";
            case "GRC":
                return "Europe";
            case "GRL":
                return "Americas";
            case "GRD":
                return "Americas";
            case "GLP":
                return "Americas";
            case "GUM":
                return "Oceania";
            case "GTM":
                return "Americas";
            case "GGY":
                return "Europe";
            case "GIN":
                return "Africa";
            case "GNB":
                return "Africa";
            case "GUY":
                return "Americas";
            case "HTI":
                return "Americas";
            case "HMD":
                return "";
            case "VAT":
                return "Europe";
            case "HND":
                return "Americas";
            case "HKG":
                return "Asia";
            case "HUN":
                return "Europe";
            case "ISL":
                return "Europe";
            case "IND":
                return "Asia";
            case "IDN":
                return "Asia";
            case "IRN":
                return "Asia";
            case "IRQ":
                return "Asia";
            case "IRL":
                return "Europe";
            case "IMN":
                return "Europe";
            case "ISR":
                return "Asia";
            case "ITA":
                return "Europe";
            case "JAM":
                return "Americas";
            case "JPN":
                return "Asia";
        }
        return "";
	}


}
