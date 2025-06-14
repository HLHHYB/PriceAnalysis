package org.example.PriceAnalysis;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * InsightReducer class
 * Comprehensive analysis of airline route and airline data to generate valuable business insights
 *
 * Analysis includes:
 * 1. Best value routes and airline recommendations
 * 2. Routes with highest price volatility (good for waiting for promotions)
 * 3. Most stable price routes (suitable for business travel)
 * 4. Airline price competitiveness analysis
 * 5. Market pricing strategy insights
 */
public class PriceInsightAnalysisReducer extends Reducer<Text, Text, Text, Text> {

    private final Text outputKey = new Text();
    private final Text outputValue = new Text();

    // Store route and airline data
    private List<RouteData> routeDataList = new ArrayList<>();
    private List<AirlineData> airlineDataList = new ArrayList<>();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String dataType = key.toString();

        if ("route".equals(dataType)) {
            processRouteData(values);
        } else if ("airline".equals(dataType)) {
            processAirlineData(values);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Generate comprehensive insight report after all data processing
        generateInsightReport(context);
    }

    private void processRouteData(Iterable<Text> values) {
        for (Text value : values) {
            try {
                String[] parts = value.toString().split("\\|");
                if (parts.length >= 2) {
                    String routeInfo = parts[0];
                    String stats = parts[1];

                    RouteData routeData = parseRouteStats(routeInfo, stats);
                    if (routeData != null) {
                        routeDataList.add(routeData);
                    }
                }
            } catch (Exception e) {
                System.err.println("Error processing route data: " + e.getMessage());
            }
        }
    }

    private void processAirlineData(Iterable<Text> values) {
        for (Text value : values) {
            try {
                String[] parts = value.toString().split("\\|");
                if (parts.length >= 2) {
                    String airlineCode = parts[0];
                    String stats = parts[1];

                    AirlineData airlineData = parseAirlineStats(airlineCode, stats);
                    if (airlineData != null) {
                        airlineDataList.add(airlineData);
                    }
                }
            } catch (Exception e) {
                System.err.println("Error processing airline data: " + e.getMessage());
            }
        }
    }

    private void generateInsightReport(Context context) throws IOException, InterruptedException {
        StringBuilder report = new StringBuilder();

        // 1. Best value routes recommendation
        outputKey.set("01_BEST_VALUE_ROUTES");
        report.append(generateBestValueRoutes());
        outputValue.set(report.toString());
        context.write(outputKey, outputValue);

        // 2. Routes with highest price volatility (promotion opportunities)
        outputKey.set("02_HIGH_VOLATILITY_ROUTES");
        report.setLength(0);
        report.append(generateHighVolatilityRoutes());
        outputValue.set(report.toString());
        context.write(outputKey, outputValue);

        // 3. Most stable price routes (preferred for business travel)
        outputKey.set("03_STABLE_PRICE_ROUTES");
        report.setLength(0);
        report.append(generateStablePriceRoutes());
        outputValue.set(report.toString());
        context.write(outputKey, outputValue);

        // 4. Airline competitiveness analysis
        outputKey.set("04_AIRLINE_COMPETITIVENESS");
        report.setLength(0);
        report.append(generateAirlineCompetitiveness());
        outputValue.set(report.toString());
        context.write(outputKey, outputValue);

        // 5. Market pricing insights
        outputKey.set("05_MARKET_PRICING_INSIGHTS");
        report.setLength(0);
        report.append(generateMarketInsights());
        outputValue.set(report.toString());
        context.write(outputKey, outputValue);

        // 6. Executive summary
        outputKey.set("06_EXECUTIVE_SUMMARY");
        report.setLength(0);
        report.append(generateExecutiveSummary());
        outputValue.set(report.toString());
        context.write(outputKey, outputValue);
    }

    private String generateBestValueRoutes() {
        StringBuilder sb = new StringBuilder();
        sb.append("=== Best Value Routes Recommendation ===\n");
        sb.append("Based on comprehensive evaluation of average price and price stability\n\n");

        // Sort by value score (low average price with relatively small standard deviation)
        routeDataList.sort((r1, r2) -> {
            double value1 = r1.avgPrice + (r1.stdDev / r1.avgPrice) * 100; // Weighted score
            double value2 = r2.avgPrice + (r2.stdDev / r2.avgPrice) * 100;
            return Double.compare(value1, value2);
        });

        int count = Math.min(10, routeDataList.size());
        for (int i = 0; i < count; i++) {
            RouteData route = routeDataList.get(i);
            sb.append(String.format("Rank %d: %s\n", i + 1, route.routeInfo));
            sb.append(String.format("  Average Price: $%.2f | Price Range: $%.2f-$%.2f\n",
                    route.avgPrice, route.minPrice, route.maxPrice));
            sb.append(String.format("  Price Stability: %.1f%% | Flight Count: %d\n",
                    (route.stdDev / route.avgPrice) * 100, route.count));
            sb.append(String.format("  Recommendation Reason: %s\n\n", getValueRecommendationReason(route)));
        }

        return sb.toString();
    }

    private String generateHighVolatilityRoutes() {
        StringBuilder sb = new StringBuilder();
        sb.append("=== High Price Volatility Routes (Promotion Opportunities) ===\n");
        sb.append("These routes have significant price variations, suitable for waiting for promotional timing\n\n");

        // Sort by price volatility rate
        routeDataList.sort((r1, r2) -> {
            double volatility1 = (r1.stdDev / r1.avgPrice) * 100;
            double volatility2 = (r2.stdDev / r2.avgPrice) * 100;
            return Double.compare(volatility2, volatility1); // Descending order
        });

        int count = Math.min(8, routeDataList.size());
        for (int i = 0; i < count; i++) {
            RouteData route = routeDataList.get(i);
            double volatility = (route.stdDev / route.avgPrice) * 100;
            if (volatility > 15) { // Only show routes with volatility > 15%
                sb.append(String.format("%s\n", route.routeInfo));
                sb.append(String.format("  Price Volatility: %.1f%% | Average Price: $%.2f\n", volatility, route.avgPrice));
                sb.append(String.format("  Lowest Price: $%.2f | Highest Price: $%.2f\n", route.minPrice, route.maxPrice));
                sb.append(String.format("  Savings Potential: Up to $%.2f savings (%.1f%%)\n\n",
                        route.avgPrice - route.minPrice,
                        ((route.avgPrice - route.minPrice) / route.avgPrice) * 100));
            }
        }

        return sb.toString();
    }

    private String generateStablePriceRoutes() {
        StringBuilder sb = new StringBuilder();
        sb.append("=== Stable Price Routes (Business Travel Preferred) ===\n");
        sb.append("These routes have relatively stable prices, suitable for business travel planning\n\n");

        // Sort by price stability (lowest volatility)
        routeDataList.sort((r1, r2) -> {
            double volatility1 = (r1.stdDev / r1.avgPrice) * 100;
            double volatility2 = (r2.stdDev / r2.avgPrice) * 100;
            return Double.compare(volatility1, volatility2); // Ascending order
        });

        int count = Math.min(8, routeDataList.size());
        for (int i = 0; i < count; i++) {
            RouteData route = routeDataList.get(i);
            double volatility = (route.stdDev / route.avgPrice) * 100;
            if (volatility < 20) { // Only show routes with volatility < 20%
                sb.append(String.format("%s\n", route.routeInfo));
                sb.append(String.format("  Price Stability: %.1f%% | Average Price: $%.2f\n", volatility, route.avgPrice));
                sb.append(String.format("  Expected Price Range: $%.2f - $%.2f\n",
                        route.avgPrice - route.stdDev, route.avgPrice + route.stdDev));
                sb.append(String.format("  Flight Frequency: %d flights\n\n", route.count));
            }
        }

        return sb.toString();
    }

    private String generateAirlineCompetitiveness() {
        StringBuilder sb = new StringBuilder();
        sb.append("=== Airline Competitiveness Analysis ===\n");
        sb.append("Comprehensive evaluation based on price level and service stability\n\n");

        // Sort by average price
        airlineDataList.sort(Comparator.comparingDouble(a -> a.avgPrice));

        sb.append("Price Competitiveness Ranking (from lowest to highest price):\n");
        for (int i = 0; i < Math.min(10, airlineDataList.size()); i++) {
            AirlineData airline = airlineDataList.get(i);
            sb.append(String.format("Rank %d: %s\n", i + 1, airline.airlineCode));
            sb.append(String.format("  Average Fare: $%.2f | Price Range: $%.2f-$%.2f\n",
                    airline.avgPrice, airline.minPrice, airline.maxPrice));
            sb.append(String.format("  Price Consistency: %.1f%% | Total Flights: %d\n",
                    (airline.stdDev / airline.avgPrice) * 100, airline.count));
            sb.append(String.format("  Market Positioning: %s\n\n", getAirlinePositioning(airline)));
        }

        return sb.toString();
    }

    private String generateMarketInsights() {
        StringBuilder sb = new StringBuilder();
        sb.append("=== Market Pricing Insights ===\n");
        sb.append("Market trends and pricing strategy insights based on data analysis\n\n");

        // Calculate overall market statistics
        double totalRoutes = routeDataList.size();
        double totalAirlines = airlineDataList.size();

        double avgRoutePrice = routeDataList.stream().mapToDouble(r -> r.avgPrice).average().orElse(0);
        double avgAirlinePrice = airlineDataList.stream().mapToDouble(a -> a.avgPrice).average().orElse(0);

        sb.append(String.format("Market Overview:\n"));
        sb.append(String.format("  Routes Analyzed: %.0f routes\n", totalRoutes));
        sb.append(String.format("  Airlines Involved: %.0f airlines\n", totalAirlines));
        sb.append(String.format("  Average Route Price: $%.2f\n", avgRoutePrice));
        sb.append(String.format("  Average Airline Price: $%.2f\n\n", avgAirlinePrice));

        // Price distribution analysis
        long lowPriceRoutes = routeDataList.stream().mapToLong(r -> r.avgPrice < avgRoutePrice * 0.8 ? 1 : 0).sum();
        long highPriceRoutes = routeDataList.stream().mapToLong(r -> r.avgPrice > avgRoutePrice * 1.2 ? 1 : 0).sum();

        sb.append("Price Distribution Insights:\n");
        sb.append(String.format("  Low-Price Routes: %.1f%% (below 20%% of market average)\n",
                (lowPriceRoutes / totalRoutes) * 100));
        sb.append(String.format("  High-Price Routes: %.1f%% (above 20%% of market average)\n",
                (highPriceRoutes / totalRoutes) * 100));
        sb.append(String.format("  Price Differentiation Level: %s\n\n",
                (lowPriceRoutes + highPriceRoutes) > totalRoutes * 0.4 ? "High" : "Moderate"));

        return sb.toString();
    }

    private String generateExecutiveSummary() {
        StringBuilder sb = new StringBuilder();
        sb.append("=== Executive Summary ===\n");
        sb.append("Practical recommendations based on comprehensive data analysis\n\n");

        if (!routeDataList.isEmpty() && !airlineDataList.isEmpty()) {
            // Best value recommendations
            RouteData bestValueRoute = routeDataList.stream()
                    .min(Comparator.comparingDouble(r -> r.avgPrice + (r.stdDev / r.avgPrice) * 100))
                    .orElse(null);

            AirlineData bestValueAirline = airlineDataList.stream()
                    .min(Comparator.comparingDouble(a -> a.avgPrice))
                    .orElse(null);

            sb.append("🏆 Top Recommendations:\n");
            if (bestValueRoute != null) {
                sb.append(String.format("  Best Route: %s (Average Price: $%.2f)\n",
                        bestValueRoute.routeInfo, bestValueRoute.avgPrice));
            }
            if (bestValueAirline != null) {
                sb.append(String.format("  Best Airline: %s (Average Price: $%.2f)\n\n",
                        bestValueAirline.airlineCode, bestValueAirline.avgPrice));
            }
        }

        sb.append("💡 Practical Advice:\n");
        sb.append("1. Budget-conscious travelers: Choose routes and airlines with the best value\n");
        sb.append("2. Business travelers: Select routes with stable pricing for cost budgeting\n");
        sb.append("3. Flexible travelers: Monitor high-volatility routes for promotional deals\n");
        sb.append("4. Regular monitoring: Recommend periodic price trend analysis to optimize travel costs\n\n");

        return sb.toString();
    }

    // Helper methods
    private RouteData parseRouteStats(String routeInfo, String stats) {
        try {
            Map<String, String> statsMap = parseStatsString(stats);
            RouteData data = new RouteData();
            data.routeInfo = routeInfo;
            data.minPrice = Double.parseDouble(statsMap.get("min"));
            data.maxPrice = Double.parseDouble(statsMap.get("max"));
            data.avgPrice = Double.parseDouble(statsMap.get("avg"));
            data.count = Integer.parseInt(statsMap.get("count"));
            data.stdDev = Double.parseDouble(statsMap.get("stdDev"));
            return data;
        } catch (Exception e) {
            return null;
        }
    }

    private AirlineData parseAirlineStats(String airlineCode, String stats) {
        try {
            Map<String, String> statsMap = parseStatsString(stats);
            AirlineData data = new AirlineData();
            data.airlineCode = airlineCode;
            data.minPrice = Double.parseDouble(statsMap.get("min"));
            data.maxPrice = Double.parseDouble(statsMap.get("max"));
            data.avgPrice = Double.parseDouble(statsMap.get("avg"));
            data.count = Integer.parseInt(statsMap.get("count"));
            data.stdDev = Double.parseDouble(statsMap.get("stdDev"));
            return data;
        } catch (Exception e) {
            return null;
        }
    }

    private Map<String, String> parseStatsString(String stats) {
        Map<String, String> map = new HashMap<>();
        String[] pairs = stats.split(",");
        for (String pair : pairs) {
            String[] kv = pair.split("=");
            if (kv.length == 2) {
                map.put(kv[0], kv[1]);
            }
        }
        return map;
    }

    private String getValueRecommendationReason(RouteData route) {
        double volatility = (route.stdDev / route.avgPrice) * 100;
        if (route.avgPrice < 200 && volatility < 15) {
            return "Low price and stable, excellent value";
        } else if (route.avgPrice < 300) {
            return "Moderate price, suitable for most travel needs";
        } else if (volatility < 10) {
            return "Stable pricing, suitable for business travel";
        } else {
            return "Good overall value";
        }
    }

    private String getAirlinePositioning(AirlineData airline) {
        if (airline.avgPrice < 250) {
            return "Budget Airline";
        } else if (airline.avgPrice < 400) {
            return "Standard Service Airline";
        } else {
            return "Premium Service Airline";
        }
    }

    // Data classes
    private static class RouteData {
        String routeInfo;
        double minPrice, maxPrice, avgPrice, stdDev;
        int count;
    }

    private static class AirlineData {
        String airlineCode;
        double minPrice, maxPrice, avgPrice, stdDev;
        int count;
    }
}