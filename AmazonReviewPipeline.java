package Amazon;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

/*
 * CSC 7740 Project: Identifying Coordinated Amazon Review Groups
 *
 * This program implements a distributed Spark-based pipeline that analyzes
 * Amazon reviews to identify potentially coordinated reviewing behavior.
 *
 * Main workflow:
 * 1. Load Amazon review and product metadata JSONL files.
 * 2. Parse, validate, and clean the review data.
 * 3. Join reviews with product metadata using the product ID.
 * 4. Group reviews for the same product within time-based buckets.
 * 5. Generate unique pairs of user accounts appearing in the same groups.
 * 6. Identify user pairs that repeatedly appear together across different
 *    product-time groups.
 * 7. Represent repeated user relationships as a weighted graph.
 * 8. Find connected components using distributed label propagation because
 *    the project uses Spark 1.3 and does not rely on GraphFrames.
 * 9. Calculate component size, edge count, graph density, repeated-group
 *    frequency, and a coordination score.
 * 10. Display candidate components for further investigation and save
 *     enriched review data in Parquet format.
 *
 * The coordination score is a prioritization signal based on observable
 * review-coordination patterns; it is not a definitive fraud classification.
 *
 * Technologies: Java, Apache Spark 1.3, Spark SQL/DataFrames, RDDs, JSONL,
 *               and Parquet.
 *
 * Team:
 * 1. Souhardya Saha Dip — ssahad1@lsu.edu
 * 2. Riyadil Zannat — rzanna2@lsu.edu
 * 3. Asif Faisal Chowdhury — achowd6@lsu.edu
 */

public class AmazonReviewPipeline {

    /*
     * Change these paths to match the files inside your VMware VM.
     */
    private static final String DEFAULT_REVIEWS_PATH =
            "/home/training/workspace/All_Beauty.jsonl";

    private static final String DEFAULT_METADATA_PATH =
            "/home/training/workspace/meta_All_Beauty.jsonl";

    private static final String DEFAULT_OUTPUT_PATH =
            "/home/training/workspace/amazon_output";

    /*
     * Candidate group parameters.
     */
    private static final int INITIAL_BUCKET_HOURS = 24;
    private static final int SMALL_BUCKET_HOURS = 6;
    private static final int OVERSIZED_GROUP_LIMIT = 100;
    private static final int MIN_REPEATED_GROUPS = 3;

    /*
     * Minimum review length for text analysis.
     */
    private static final int MIN_TEXT_LENGTH = 30;

    /*
     * Maximum number of connected-component iterations.
     */
    private static final int COMPONENT_ITERATIONS = 20;


    public static void main(String[] args) {

        String reviewsPath =
                args.length > 0
                        ? args[0]
                        : DEFAULT_REVIEWS_PATH;

        String metadataPath =
                args.length > 1
                        ? args[1]
                        : DEFAULT_METADATA_PATH;

        String outputPath =
                args.length > 2
                        ? args[2]
                        : DEFAULT_OUTPUT_PATH;


      
        SparkConf conf =
                new SparkConf()
                        .setAppName(
                                "Amazon Review Coordination Analysis"
                        )
                        .setMaster("local[*]");


        JavaSparkContext sc =
                new JavaSparkContext(conf);

        SQLContext sqlContext =
                new SQLContext(sc);


        try {

            System.out.println(
                    "Starting Amazon Review Analysis..."
            );


            /*
             * ====================================================
             * 1. LOAD REVIEWS
             * ====================================================
             */
            DataFrame reviewDF =
                    sqlContext.jsonFile(
                            reviewsPath
                    );

            JavaRDD<Row> reviewRows =
                    reviewDF.javaRDD();


            /*
             * ====================================================
             * 2. LOAD PRODUCT METADATA
             * ====================================================
             */
            DataFrame metadataDF =
                    sqlContext.jsonFile(
                            metadataPath
                    );

            JavaRDD<Row> metadataRows =
                    metadataDF.javaRDD();


            /*
             * ====================================================
             * 3. PARSE AND CLEAN REVIEWS
             * ====================================================
             */
            JavaRDD<Review> reviews =
                    reviewRows
                            .map(
                                    new Function<Row, Review>() {

                                        private static final long serialVersionUID =
                                                1L;

                                        public Review call(
                                                Row row) {

                                            return Review.fromRow(
                                                    row
                                            );
                                        }
                                    }
                            )
                            .filter(
                                    new Function<Review, Boolean>() {

                                        private static final long serialVersionUID =
                                                1L;

                                        public Boolean call(
                                                Review review) {

                                            return review != null
                                                    && review.userId != null
                                                    && review.productId != null
                                                    && review.timestamp > 0;
                                        }
                                    }
                            )
                            .map(
                                    new Function<Review, Review>() {

                                        private static final long serialVersionUID =
                                                1L;

                                        public Review call(
                                                Review review) {

                                            review.reviewText =
                                                    cleanText(
                                                            review.reviewText
                                                    );

                                            return review;
                                        }
                                    }
                            );


            /*
             * ====================================================
             * 4. PARSE PRODUCT METADATA
             * ====================================================
             */
            JavaPairRDD<String, ProductMetadata>
                    metadataByProduct =
                    metadataRows
                            .mapToPair(
                                    new PairFunction<
                                            Row,
                                            String,
                                            ProductMetadata>() {

                                        private static final long serialVersionUID =
                                                1L;

                                        public Tuple2<
                                                String,
                                                ProductMetadata> call(
                                                Row row) {

                                            ProductMetadata metadata =
                                                    ProductMetadata.fromRow(
                                                            row
                                                    );

                                            return new Tuple2<
                                                    String,
                                                    ProductMetadata>(
                                                    metadata.productId,
                                                    metadata
                                            );
                                        }
                                    }
                            )
                            .filter(
                                    new Function<
                                            Tuple2<
                                                    String,
                                                    ProductMetadata>,
                                            Boolean>() {

                                        private static final long serialVersionUID =
                                                1L;

                                        public Boolean call(
                                                Tuple2<
                                                        String,
                                                        ProductMetadata> value) {

                                            return value._1() != null;
                                        }
                                    }
                            );


            /*
             * ====================================================
             * 5. JOIN REVIEWS WITH PRODUCT METADATA
             * ====================================================
             */
            JavaPairRDD<String, Review>
                    reviewsByProduct =
                    reviews.mapToPair(
                            new PairFunction<
                                    Review,
                                    String,
                                    Review>() {

                                private static final long serialVersionUID =
                                        1L;

                                public Tuple2<String, Review> call(
                                        Review review) {

                                    return new Tuple2<
                                            String,
                                            Review>(
                                            review.productId,
                                            review
                                    );
                                }
                            }
                    );


            JavaPairRDD<
                    String,
                    Tuple2<Review, ProductMetadata>>
                    joined =
                    reviewsByProduct.join(
                            metadataByProduct
                    );


            JavaRDD<EnrichedReview>
                    enrichedReviews =
                    joined.values().map(
                            new Function<
                                    Tuple2<
                                            Review,
                                            ProductMetadata>,
                                    EnrichedReview>() {

                                private static final long serialVersionUID =
                                        1L;

                                public EnrichedReview call(
                                        Tuple2<
                                                Review,
                                                ProductMetadata> value) {

                                    return new EnrichedReview(
                                            value._1(),
                                            value._2()
                                    );
                                }
                            }
                    );


            /*
             * ====================================================
             * 6. SAVE ENRICHED DATA AS PARQUET
             * ====================================================
             */
            writeParquet(
                    sqlContext,
                    enrichedReviews,
                    outputPath
            );


            /*
             * ====================================================
             * 7. CREATE PRODUCT-TIME GROUPS
             *
             * Reviews of the same product inside a 24-hour
             * bucket become candidate co-review groups.
             * ====================================================
             */
            JavaPairRDD<
                    String,
                    EnrichedReview>
                    productTime =
                    enrichedReviews.mapToPair(
                            new PairFunction<
                                    EnrichedReview,
                                    String,
                                    EnrichedReview>() {

                                private static final long serialVersionUID =
                                        1L;

                                public Tuple2<
                                        String,
                                        EnrichedReview> call(
                                        EnrichedReview review) {

                                    long bucket =
                                            getTimeBucket(
                                                    review.review.timestamp,
                                                    INITIAL_BUCKET_HOURS
                                            );

                                    String key =
                                            review.review.productId
                                                    + "\t"
                                                    + bucket;

                                    return new Tuple2<
                                            String,
                                            EnrichedReview>(
                                            key,
                                            review
                                    );
                                }
                            }
                    );


            JavaPairRDD<
                    String,
                    Iterable<EnrichedReview>>
                    grouped =
                    productTime.groupByKey();


            /*
             * ====================================================
             * 8. SPLIT OVERSIZED GROUPS
             *
             * Groups with more than 100 reviews are divided
             * into six-hour buckets.
             * ====================================================
             */
            JavaPairRDD<
                    String,
                    EnrichedReview>
                    candidateReviews =
                    grouped.flatMapToPair(
                            new PairFlatMapFunction<
                                    Tuple2<
                                            String,
                                            Iterable<EnrichedReview>>,
                                    String,
                                    EnrichedReview>() {

                                private static final long serialVersionUID =
                                        1L;

                                public Iterable<
                                        Tuple2<String, EnrichedReview>>
                                call(
                                        Tuple2<
                                                String,
                                                Iterable<EnrichedReview>>
                                                value) {

                                    List<EnrichedReview> list =
                                            new ArrayList<
                                                    EnrichedReview>();

                                    Iterator<EnrichedReview> iterator =
                                            value._2().iterator();

                                    while (iterator.hasNext()) {

                                        list.add(
                                                iterator.next()
                                        );
                                    }


                                    List<
                                            Tuple2<String, EnrichedReview>>
                                            output =
                                            new ArrayList<
                                                    Tuple2<String, EnrichedReview>>();


                                    if (list.size()
                                            <= OVERSIZED_GROUP_LIMIT) {

                                        for (EnrichedReview review :
                                                list) {

                                            output.add(
                                                    new Tuple2<
                                                            String,
                                                            EnrichedReview>(
                                                            value._1(),
                                                            review
                                                    )
                                            );
                                        }

                                    } else {

                                        for (EnrichedReview review :
                                                list) {

                                            long bucket =
                                                    getTimeBucket(
                                                            review.review.timestamp,
                                                            SMALL_BUCKET_HOURS
                                                    );

                                            String key =
                                                    review.review.productId
                                                            + "\t"
                                                            + bucket;

                                            output.add(
                                                    new Tuple2<
                                                            String,
                                                            EnrichedReview>(
                                                            key,
                                                            review
                                                    )
                                            );
                                        }
                                    }

                                    return output;
                                }
                            }
                    );


            JavaPairRDD<
                    String,
                    Iterable<EnrichedReview>>
                    candidateGroups =
                    candidateReviews.groupByKey();


            /*
             * ====================================================
             * 9. GENERATE ACCOUNT PAIRS
             *
             * IMPORTANT:
             *
             * Spark 1.3 flatMapToPair() requires
             * PairFlatMapFunction, not PairFunction.
             * ====================================================
             */
            JavaPairRDD<String, PairRecord>
                    pairRecords =
                    candidateGroups.flatMapToPair(
                            new PairFlatMapFunction<
                                    Tuple2<
                                            String,
                                            Iterable<EnrichedReview>>,
                                    String,
                                    PairRecord>() {

                                private static final long serialVersionUID =
                                        1L;

                                public Iterable<
                                        Tuple2<String, PairRecord>>
                                call(
                                        Tuple2<
                                                String,
                                                Iterable<EnrichedReview>>
                                                value) {

                                    Set<String> uniqueUsers =
                                            new HashSet<String>();

                                    Iterator<EnrichedReview> iterator =
                                            value._2().iterator();

                                    while (iterator.hasNext()) {

                                        EnrichedReview review =
                                                iterator.next();

                                        if (review.review.userId != null) {

                                            uniqueUsers.add(
                                                    review.review.userId
                                            );
                                        }
                                    }


                                    List<String> users =
                                            new ArrayList<String>(
                                                    uniqueUsers
                                            );

                                    Collections.sort(
                                            users
                                    );


                                    List<
                                            Tuple2<String, PairRecord>>
                                            result =
                                            new ArrayList<
                                                    Tuple2<String, PairRecord>>();


                                    for (int i = 0;
                                         i < users.size();
                                         i++) {

                                        for (int j = i + 1;
                                             j < users.size();
                                             j++) {

                                            String userA =
                                                    users.get(i);

                                            String userB =
                                                    users.get(j);

                                            String pairKey =
                                                    userA
                                                            + "\t"
                                                            + userB;


                                            result.add(
                                                    new Tuple2<
                                                            String,
                                                            PairRecord>(
                                                            pairKey,
                                                            new PairRecord(
                                                                    userA,
                                                                    userB,
                                                                    value._1()
                                                            )
                                                    )
                                            );
                                        }
                                    }

                                    return result;
                                }
                            }
                    );


            /*
             * ====================================================
             * 10. FIND REPEATED ACCOUNT PAIRS
             *
             * An edge is created only when two accounts appear
             * together in at least three distinct product-time
             * groups.
             * ====================================================
             */
            JavaPairRDD<
                    String,
                    Iterable<PairRecord>>
                    pairGroups =
                    pairRecords.groupByKey();


            JavaPairRDD<String, Edge>
                    edges =
                    pairGroups
                            .mapToPair(
                                    new PairFunction<
                                            Tuple2<
                                                    String,
                                                    Iterable<PairRecord>>,
                                            String,
                                            Edge>() {

                                        private static final long serialVersionUID =
                                                1L;

                                        public Tuple2<
                                                String,
                                                Edge> call(
                                                Tuple2<
                                                        String,
                                                        Iterable<PairRecord>>
                                                        value) {

                                            Set<String> groups =
                                                    new HashSet<String>();

                                            String userA = null;
                                            String userB = null;


                                            Iterator<PairRecord> iterator =
                                                    value._2().iterator();

                                            while (iterator.hasNext()) {

                                                PairRecord record =
                                                        iterator.next();

                                                userA =
                                                        record.userA;

                                                userB =
                                                        record.userB;

                                                groups.add(
                                                        record.groupKey
                                                );
                                            }


                                            Edge edge =
                                                    new Edge(
                                                            userA,
                                                            userB,
                                                            groups.size()
                                                    );


                                            return new Tuple2<
                                                    String,
                                                    Edge>(
                                                    value._1(),
                                                    edge
                                            );
                                        }
                                    }
                            )
                            .filter(
                                    new Function<
                                            Tuple2<String, Edge>,
                                            Boolean>() {

                                        private static final long serialVersionUID =
                                                1L;

                                        public Boolean call(
                                                Tuple2<String, Edge> value) {

                                            return value._2().weight
                                                    >= MIN_REPEATED_GROUPS;
                                        }
                                    }
                            );


            /*
             * ====================================================
             * 11. CREATE GRAPH VERTICES
             * ====================================================
             */
            JavaRDD<String>
                    vertices =
                    edges.flatMap(
                            new FlatMapFunction<
                                    Tuple2<String, Edge>,
                                    String>() {

                                private static final long serialVersionUID =
                                        1L;

                                public Iterable<String> call(
                                        Tuple2<String, Edge> value) {

                                    List<String> users =
                                            new ArrayList<String>();

                                    users.add(
                                            value._2().userA
                                    );

                                    users.add(
                                            value._2().userB
                                    );

                                    return users;
                                }
                            }
                    ).distinct();


            JavaPairRDD<String, String>
                    initialLabels =
                    vertices.mapToPair(
                            new PairFunction<
                                    String,
                                    String,
                                    String>() {

                                private static final long serialVersionUID =
                                        1L;

                                public Tuple2<String, String> call(
                                        String user) {

                                    return new Tuple2<
                                            String,
                                            String>(
                                            user,
                                            user
                                    );
                                }
                            }
                    );


            /*
             * ====================================================
             * 12. CONNECTED COMPONENTS
             *
             * GraphFrames is not used because this project is
             * running on Spark 1.3.
             *
             * Instead, component labels are propagated through
             * the distributed graph.
             * ====================================================
             */
            JavaPairRDD<String, String>
                    components =
                    connectedComponents(
                            initialLabels,
                            edges
                    );


            /*
             * ====================================================
             * 13. COUNT USERS PER COMPONENT
             * ====================================================
             */
            JavaPairRDD<String, Integer>
                    componentSizes =
                    components
                            .mapToPair(
                                    new PairFunction<
                                            Tuple2<String, String>,
                                            String,
                                            Integer>() {

                                        private static final long serialVersionUID =
                                                1L;

                                        public Tuple2<
                                                String,
                                                Integer> call(
                                                Tuple2<
                                                        String,
                                                        String> value) {

                                            return new Tuple2<
                                                    String,
                                                    Integer>(
                                                    value._2(),
                                                    1
                                            );
                                        }
                                    }
                            )
                            .reduceByKey(
                                    new Function2<
                                            Integer,
                                            Integer,
                                            Integer>() {

                                        private static final long serialVersionUID =
                                                1L;

                                        public Integer call(
                                                Integer a,
                                                Integer b) {

                                            return a + b;
                                        }
                                    }
                            );


            /*
             * ====================================================
             * 14. ASSIGN EDGES TO COMPONENTS
             * ====================================================
             */
            JavaPairRDD<String, Edge>
                    edgesByUser =
                    edges.mapToPair(
                            new PairFunction<
                                    Tuple2<String, Edge>,
                                    String,
                                    Edge>() {

                                private static final long serialVersionUID =
                                        1L;

                                public Tuple2<String, Edge> call(
                                        Tuple2<String, Edge> value) {

                                    return new Tuple2<
                                            String,
                                            Edge>(
                                            value._2().userA,
                                            value._2()
                                    );
                                }
                            }
                    );


            JavaPairRDD<
                    String,
                    Tuple2<Edge, String>>
                    edgeComponents =
                    edgesByUser.join(
                            components
                    );


            /*
             * ====================================================
             * 15. AGGREGATE COMPONENT METRICS
             * ====================================================
             */
            JavaPairRDD<String, ComponentMetrics>
                    metrics =
                    edgeComponents.mapToPair(
                            new PairFunction<
                                    Tuple2<
                                            String,
                                            Tuple2<Edge, String>>,
                                    String,
                                    ComponentMetrics>() {

                                private static final long serialVersionUID =
                                        1L;

                                public Tuple2<
                                        String,
                                        ComponentMetrics> call(
                                        Tuple2<
                                                String,
                                                Tuple2<Edge, String>>
                                                value) {

                                    Edge edge =
                                            value._2()._1();

                                    String component =
                                            value._2()._2();

                                    ComponentMetrics result =
                                            new ComponentMetrics();

                                    result.componentId =
                                            component;

                                    result.edgeCount =
                                            1;

                                    result.totalRepeatedGroups =
                                            edge.weight;

                                    return new Tuple2<
                                            String,
                                            ComponentMetrics>(
                                            component,
                                            result
                                    );
                                }
                            }
                    )
                    .reduceByKey(
                            new Function2<
                                    ComponentMetrics,
                                    ComponentMetrics,
                                    ComponentMetrics>() {

                                private static final long serialVersionUID =
                                        1L;

                                public ComponentMetrics call(
                                        ComponentMetrics a,
                                        ComponentMetrics b) {

                                    a.edgeCount +=
                                            b.edgeCount;

                                    a.totalRepeatedGroups +=
                                            b.totalRepeatedGroups;

                                    return a;
                                }
                            }
                    );


            /*
             * ====================================================
             * 16. JOIN COMPONENT SIZE WITH METRICS
             * ====================================================
             */
            JavaPairRDD<
                    String,
                    Tuple2<ComponentMetrics, Integer>>
                    finalMetrics =
                    metrics.join(
                            componentSizes
                    );


            /*
             * ====================================================
             * 17. CALCULATE COORDINATION SCORE
             *
             * The score is a prioritization signal and not a
             * definitive fraud classification.
             * ====================================================
             */
            JavaRDD<ComponentResult>
                    results =
                    finalMetrics.values().map(
                            new Function<
                                    Tuple2<
                                            ComponentMetrics,
                                            Integer>,
                                    ComponentResult>() {

                                private static final long serialVersionUID =
                                        1L;

                                public ComponentResult call(
                                        Tuple2<
                                                ComponentMetrics,
                                                Integer>
                                                value) {

                                    ComponentMetrics metrics =
                                            value._1();

                                    int userCount =
                                            value._2();


                                    double possibleEdges =
                                            userCount <= 1
                                                    ? 1.0
                                                    : (
                                                            (double) userCount
                                                                    * (userCount - 1)
                                                      ) / 2.0;


                                    double density =
                                            metrics.edgeCount
                                                    / possibleEdges;


                                    double averageRepetition =
                                            metrics.edgeCount == 0
                                                    ? 0.0
                                                    : (
                                                            (double)
                                                                    metrics.totalRepeatedGroups
                                                                    / metrics.edgeCount
                                                      );


                                    double sizeSignal =
                                            Math.min(
                                                    userCount / 20.0,
                                                    1.0
                                            );


                                    double repetitionSignal =
                                            Math.min(
                                                    averageRepetition / 10.0,
                                                    1.0
                                            );


                                    double score =
                                              0.35 * sizeSignal
                                            + 0.35 * density
                                            + 0.30 * repetitionSignal;


                                    return new ComponentResult(
                                            metrics.componentId,
                                            userCount,
                                            metrics.edgeCount,
                                            metrics.totalRepeatedGroups,
                                            density,
                                            averageRepetition,
                                            score
                                    );
                                }
                            }
                    );


            /*
             * ====================================================
             * 18. SORT AND DISPLAY RESULTS
             * ====================================================
             */
            List<ComponentResult>
                    resultList =
                    results.collect();


            Collections.sort(
                    resultList,
                    new Comparator<ComponentResult>() {

                        public int compare(
                                ComponentResult a,
                                ComponentResult b) {

                            return Double.compare(
                                    b.coordinationScore,
                                    a.coordinationScore
                            );
                        }
                    }
            );


            System.out.println();
            System.out.println(
                    "=========================================="
            );
            System.out.println(
                    "Amazon Review Coordination Analysis"
            );
            System.out.println(
                    "=========================================="
            );

            System.out.println(
                    "Candidate components: "
                            + resultList.size()
            );


            int limit =
                    Math.min(
                            20,
                            resultList.size()
                    );


            for (int i = 0;
                 i < limit;
                 i++) {

                ComponentResult result =
                        resultList.get(i);

                System.out.println(
                        "Component="
                                + result.componentId
                                + " | Users="
                                + result.userCount
                                + " | Edges="
                                + result.edgeCount
                                + " | RepeatedGroups="
                                + result.totalRepeatedGroups
                                + " | Density="
                                + result.edgeDensity
                                + " | AvgRepetition="
                                + result.averageRepetition
                                + " | CoordinationScore="
                                + result.coordinationScore
                );
            }


            System.out.println();
            System.out.println(
                    "Analysis completed successfully."
            );


        } finally {

            sc.close();
        }
    }


    /*
     * ============================================================
     * CONNECTED COMPONENTS
     * ============================================================
     */
    private static JavaPairRDD<String, String>
    connectedComponents(
            JavaPairRDD<String, String> labels,
            JavaPairRDD<String, Edge> edges) {

        JavaPairRDD<String, String>
                current =
                labels;


        for (int iteration = 0;
             iteration < COMPONENT_ITERATIONS;
             iteration++) {


            /*
             * Send every edge in both directions.
             */
            JavaPairRDD<String, String>
                    neighborRequests =
                    edges.flatMapToPair(
                            new PairFlatMapFunction<
                                    Tuple2<String, Edge>,
                                    String,
                                    String>() {

                                private static final long serialVersionUID =
                                        1L;

                                public Iterable<
                                        Tuple2<String, String>>
                                call(
                                        Tuple2<String, Edge> value) {

                                    Edge edge =
                                            value._2();

                                    List<
                                            Tuple2<String, String>>
                                            requests =
                                            new ArrayList<
                                                    Tuple2<String, String>>();

                                    requests.add(
                                            new Tuple2<
                                                    String,
                                                    String>(
                                                    edge.userA,
                                                    edge.userB
                                            )
                                    );

                                    requests.add(
                                            new Tuple2<
                                                    String,
                                                    String>(
                                                    edge.userB,
                                                    edge.userA
                                            )
                                    );

                                    return requests;
                                }
                            }
                    );


            /*
             * Obtain the current label of every neighbor.
             */
            JavaPairRDD<
                    String,
                    Tuple2<String, String>>
                    neighborLabels =
                    neighborRequests.join(
                            current
                    );


            /*
             * user -> neighbor component label.
             */
            JavaPairRDD<String, String>
                    proposals =
                    neighborLabels.mapToPair(
                            new PairFunction<
                                    Tuple2<
                                            String,
                                            Tuple2<String, String>>,
                                    String,
                                    String>() {

                                private static final long serialVersionUID =
                                        1L;

                                public Tuple2<
                                        String,
                                        String> call(
                                        Tuple2<
                                                String,
                                                Tuple2<String, String>>
                                                value) {

                                    return new Tuple2<
                                            String,
                                            String>(
                                            value._1(),
                                            value._2()._2()
                                    );
                                }
                            }
                    );


            /*
             * Select the smallest component label.
             */
            JavaPairRDD<String, String>
                    minimumLabels =
                    proposals.reduceByKey(
                            new Function2<
                                    String,
                                    String,
                                    String>() {

                                private static final long serialVersionUID =
                                        1L;

                                public String call(
                                        String a,
                                        String b) {

                                    return a.compareTo(b) <= 0
                                            ? a
                                            : b;
                                }
                            }
                    );


            /*
             * Compare current and proposed labels.
             */
            JavaPairRDD<
                    String,
                    Tuple2<String, String>>
                    combined =
                    current.join(
                            minimumLabels
                    );


            JavaPairRDD<String, String>
                    updated =
                    combined.mapToPair(
                            new PairFunction<
                                    Tuple2<
                                            String,
                                            Tuple2<String, String>>,
                                    String,
                                    String>() {

                                private static final long serialVersionUID =
                                        1L;

                                public Tuple2<
                                        String,
                                        String> call(
                                        Tuple2<
                                                String,
                                                Tuple2<String, String>>
                                                value) {

                                    String currentLabel =
                                            value._2()._1();

                                    String proposedLabel =
                                            value._2()._2();

                                    String newLabel =
                                            currentLabel.compareTo(
                                                    proposedLabel
                                            ) <= 0
                                                    ? currentLabel
                                                    : proposedLabel;

                                    return new Tuple2<
                                            String,
                                            String>(
                                            value._1(),
                                            newLabel
                                    );
                                }
                            }
                    );


            /*
             * Preserve vertices without neighbors.
             */
            JavaPairRDD<String, String>
                    unchanged =
                    current.subtractByKey(
                            updated
                    );


            current =
                    updated
                            .union(
                                    unchanged
                            )
                            .reduceByKey(
                                    new Function2<
                                            String,
                                            String,
                                            String>() {

                                        private static final long serialVersionUID =
                                                1L;

                                        public String call(
                                                String a,
                                                String b) {

                                            return a.compareTo(b) <= 0
                                                    ? a
                                                    : b;
                                        }
                                    }
                            );
        }


        return current;
    }


    /*
     * ============================================================
     * CLEAN REVIEW TEXT
     * ============================================================
     */
    private static String cleanText(
            String text) {

        if (text == null) {
            return "";
        }

        String cleaned =
                text.toLowerCase();

        cleaned =
                cleaned.replaceAll(
                        "[^a-z0-9\\s]",
                        " "
                );

        cleaned =
                cleaned.replaceAll(
                        "\\s+",
                        " "
                );

        return cleaned.trim();
    }


    /*
     * ============================================================
     * TIME BUCKET
     * ============================================================
     */
    private static long getTimeBucket(
            long timestamp,
            int hours) {

        long secondsPerBucket =
                hours
                        * 60L
                        * 60L;

        return timestamp /
                secondsPerBucket;
    }


    /*
     * ============================================================
     * PARQUET OUTPUT
     * ============================================================
     */
    private static void writeParquet(
            SQLContext sqlContext,
            JavaRDD<EnrichedReview> reviews,
            String outputPath) {

        JavaRDD<Row>
                rows =
                reviews.map(
                        new Function<
                                EnrichedReview,
                                Row>() {

                            private static final long serialVersionUID =
                                    1L;

                            public Row call(
                                    EnrichedReview value) {

                                return RowFactory.create(
                                        value.review.userId,
                                        value.review.productId,
                                        value.review.rating,
                                        value.review.timestamp,
                                        value.review.reviewText,
                                        value.review.verifiedPurchase,
                                        value.review.helpfulVotes,
                                        value.metadata.category,
                                        value.metadata.productTitle,
                                        value.metadata.catalogAverageRating
                                );
                            }
                        }
                );


        List<StructField>
                fields =
                new ArrayList<StructField>();


        fields.add(
                DataTypes.createStructField(
                        "user_id",
                        DataTypes.StringType,
                        true
                )
        );


        fields.add(
                DataTypes.createStructField(
                        "product_id",
                        DataTypes.StringType,
                        true
                )
        );


        fields.add(
                DataTypes.createStructField(
                        "rating",
                        DataTypes.DoubleType,
                        true
                )
        );


        fields.add(
                DataTypes.createStructField(
                        "timestamp",
                        DataTypes.LongType,
                        true
                )
        );


        fields.add(
                DataTypes.createStructField(
                        "review_text",
                        DataTypes.StringType,
                        true
                )
        );


        fields.add(
                DataTypes.createStructField(
                        "verified_purchase",
                        DataTypes.BooleanType,
                        true
                )
        );


        fields.add(
                DataTypes.createStructField(
                        "helpful_votes",
                        DataTypes.IntegerType,
                        true
                )
        );


        fields.add(
                DataTypes.createStructField(
                        "category",
                        DataTypes.StringType,
                        true
                )
        );


        fields.add(
                DataTypes.createStructField(
                        "product_title",
                        DataTypes.StringType,
                        true
                )
        );


        fields.add(
                DataTypes.createStructField(
                        "catalog_average_rating",
                        DataTypes.DoubleType,
                        true
                )
        );


        StructType schema =
                DataTypes.createStructType(
                        fields
                );


        DataFrame output =
                sqlContext.createDataFrame(
                        rows,
                        schema
                );


        output.saveAsParquetFile(outputPath);
    }


    /*
     * ============================================================
     * REVIEW CLASS
     * ============================================================
     */
    public static class Review
            implements Serializable {

        private static final long serialVersionUID =
                1L;

        String userId;
        String productId;
        double rating;
        long timestamp;
        String reviewText;
        boolean verifiedPurchase;
        int helpfulVotes;


        Review(
                String userId,
                String productId,
                double rating,
                long timestamp,
                String reviewText,
                boolean verifiedPurchase,
                int helpfulVotes) {

            this.userId =
                    userId;

            this.productId =
                    productId;

            this.rating =
                    rating;

            this.timestamp =
                    timestamp;

            this.reviewText =
                    reviewText;

            this.verifiedPurchase =
                    verifiedPurchase;

            this.helpfulVotes =
                    helpfulVotes;
        }


        static Review fromRow(
                Row row) {

            try {

                String userId =
                        getString(
                                row,
                                "user_id"
                        );

                if (userId == null) {
                    userId =
                            getString(
                                    row,
                                    "user"
                            );
                }


                String productId =
                        getString(
                                row,
                                "parent_asin"
                        );

                if (productId == null) {
                    productId =
                            getString(
                                    row,
                                    "asin"
                            );
                }

                if (productId == null) {
                    productId =
                            getString(
                                    row,
                                    "product_id"
                            );
                }


                String reviewText =
                        getString(
                                row,
                                "text"
                        );

                if (reviewText == null) {
                    reviewText =
                            getString(
                                    row,
                                    "reviewText"
                            );
                }


                double rating =
                        getDouble(
                                row,
                                "rating",
                                0.0
                        );


                long timestamp =
                        getLong(
                                row,
                                "timestamp",
                                0L
                        );


                boolean verifiedPurchase =
                        getBoolean(
                                row,
                                "verified_purchase",
                                false
                        );

                if (!verifiedPurchase) {
                    verifiedPurchase =
                            getBoolean(
                                    row,
                                    "verifiedPurchase",
                                    false
                            );
                }


                int helpfulVotes =
                        getInt(
                                row,
                                "helpful_vote",
                                0
                        );

                if (helpfulVotes == 0) {
                    helpfulVotes =
                            getInt(
                                    row,
                                    "helpful_votes",
                                    0
                            );
                }


                /*
                 * Convert milliseconds to seconds when necessary.
                 */
                if (timestamp >
                        100000000000L) {

                    timestamp =
                            timestamp / 1000L;
                }


                return new Review(
                        userId,
                        productId,
                        rating,
                        timestamp,
                        reviewText,
                        verifiedPurchase,
                        helpfulVotes
                );

            } catch (Exception e) {

                return null;
            }
        }
    }


    /*
     * ============================================================
     * PRODUCT METADATA CLASS
     * ============================================================
     */
    public static class ProductMetadata
            implements Serializable {

        private static final long serialVersionUID =
                1L;

        String productId;
        String category;
        String productTitle;
        double catalogAverageRating;


        ProductMetadata(
                String productId,
                String category,
                String productTitle,
                double catalogAverageRating) {

            this.productId =
                    productId;

            this.category =
                    category;

            this.productTitle =
                    productTitle;

            this.catalogAverageRating =
                    catalogAverageRating;
        }


        static ProductMetadata fromRow(
                Row row) {

            String productId =
                    getString(
                            row,
                            "parent_asin"
                    );

            if (productId == null) {
                productId =
                        getString(
                                row,
                                "asin"
                        );
            }

            if (productId == null) {
                productId =
                        getString(
                                row,
                                "product_id"
                        );
            }


            String category =
                    getString(
                            row,
                            "main_category"
                    );

            if (category == null) {
                category =
                        getString(
                                row,
                                "category"
                        );
            }


            String title =
                    getString(
                            row,
                            "title"
                    );


            double averageRating =
                    getDouble(
                            row,
                            "average_rating",
                            0.0
                    );


            return new ProductMetadata(
                    productId,
                    category,
                    title,
                    averageRating
            );
        }
    }


    /*
     * ============================================================
     * ENRICHED REVIEW CLASS
     * ============================================================
     */
    public static class EnrichedReview
            implements Serializable {

        private static final long serialVersionUID =
                1L;

        Review review;
        ProductMetadata metadata;


        EnrichedReview(
                Review review,
                ProductMetadata metadata) {

            this.review =
                    review;

            this.metadata =
                    metadata;
        }
    }


    /*
     * ============================================================
     * ACCOUNT PAIR RECORD
     * ============================================================
     */
    public static class PairRecord
            implements Serializable {

        private static final long serialVersionUID =
                1L;

        String userA;
        String userB;
        String groupKey;


        PairRecord(
                String userA,
                String userB,
                String groupKey) {

            this.userA =
                    userA;

            this.userB =
                    userB;

            this.groupKey =
                    groupKey;
        }
    }


    /*
     * ============================================================
     * GRAPH EDGE
     * ============================================================
     */
    public static class Edge
            implements Serializable {

        private static final long serialVersionUID =
                1L;

        String userA;
        String userB;
        int weight;


        Edge(
                String userA,
                String userB,
                int weight) {

            this.userA =
                    userA;

            this.userB =
                    userB;

            this.weight =
                    weight;
        }
    }


    /*
     * ============================================================
     * COMPONENT METRICS
     * ============================================================
     */
    public static class ComponentMetrics
            implements Serializable {

        private static final long serialVersionUID =
                1L;

        String componentId;
        int edgeCount;
        int totalRepeatedGroups;
    }


    /*
     * ============================================================
     * FINAL COMPONENT RESULT
     * ============================================================
     */
    public static class ComponentResult
            implements Serializable {

        private static final long serialVersionUID =
                1L;

        String componentId;
        int userCount;
        int edgeCount;
        int totalRepeatedGroups;
        double edgeDensity;
        double averageRepetition;
        double coordinationScore;


        ComponentResult(
                String componentId,
                int userCount,
                int edgeCount,
                int totalRepeatedGroups,
                double edgeDensity,
                double averageRepetition,
                double coordinationScore) {

            this.componentId =
                    componentId;

            this.userCount =
                    userCount;

            this.edgeCount =
                    edgeCount;

            this.totalRepeatedGroups =
                    totalRepeatedGroups;

            this.edgeDensity =
                    edgeDensity;

            this.averageRepetition =
                    averageRepetition;

            this.coordinationScore =
                    coordinationScore;
        }
    }


    /*
     * ============================================================
     * ROW HELPERS
     * ============================================================
     */
    private static int fieldIndex(Row row, String field) {
        String[] fieldNames = row.schema().fieldNames();

        for (int i = 0; i < fieldNames.length; i++) {
            if (fieldNames[i].equals(field)) {
                return i;
            }
        }

        return -1;
    }


    private static String getString(
            Row row,
            String field) {

        int index =
                fieldIndex(
                        row,
                        field
                );

        if (index < 0) {
            return null;
        }

        try {

            if (row.isNullAt(index)) {
                return null;
            }

            Object value =
                    row.get(index);

            return value == null
                    ? null
                    : value.toString();

        } catch (Exception e) {

            return null;
        }
    }


    private static double getDouble(
            Row row,
            String field,
            double defaultValue) {

        int index =
                fieldIndex(
                        row,
                        field
                );

        if (index < 0) {
            return defaultValue;
        }

        try {

            if (row.isNullAt(index)) {
                return defaultValue;
            }

            Object value =
                    row.get(index);

            if (value instanceof Number) {

                return (
                        (Number) value
                ).doubleValue();
            }

            return Double.parseDouble(
                    value.toString()
            );

        } catch (Exception e) {

            return defaultValue;
        }
    }


    private static long getLong(
            Row row,
            String field,
            long defaultValue) {

        int index =
                fieldIndex(
                        row,
                        field
                );

        if (index < 0) {
            return defaultValue;
        }

        try {

            if (row.isNullAt(index)) {
                return defaultValue;
            }

            Object value =
                    row.get(index);

            if (value instanceof Number) {

                return (
                        (Number) value
                ).longValue();
            }

            return Long.parseLong(
                    value.toString()
            );

        } catch (Exception e) {

            return defaultValue;
        }
    }


    private static int getInt(
            Row row,
            String field,
            int defaultValue) {

        int index =
                fieldIndex(
                        row,
                        field
                );

        if (index < 0) {
            return defaultValue;
        }

        try {

            if (row.isNullAt(index)) {
                return defaultValue;
            }

            Object value =
                    row.get(index);

            if (value instanceof Number) {

                return (
                        (Number) value
                ).intValue();
            }

            return Integer.parseInt(
                    value.toString()
            );

        } catch (Exception e) {

            return defaultValue;
        }
    }


    private static boolean getBoolean(
            Row row,
            String field,
            boolean defaultValue) {

        int index =
                fieldIndex(
                        row,
                        field
                );

        if (index < 0) {
            return defaultValue;
        }

        try {

            if (row.isNullAt(index)) {
                return defaultValue;
            }

            Object value =
                    row.get(index);

            if (value instanceof Boolean) {

                return (
                        (Boolean) value
                ).booleanValue();
            }

            return Boolean.parseBoolean(
                    value.toString()
            );

        } catch (Exception e) {

            return defaultValue;
        }
    }
}