use chrono::{DateTime, Utc};
use clap::Parser;
use memuse::DynamicUsage;
use rand::Rng;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use uuid::Uuid;

// Command line arguments
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Number of documents to generate
    #[arg(short, long, default_value_t = 10_000_000)]
    num_docs: usize,

    /// Percentage of documents to include in filtered query (0-100)
    #[arg(short, long, default_value_t = 10)]
    filter_percentage: usize,

    /// Leaf size for AIT
    #[arg(short, long, default_value_t = 64)]
    leaf_size: usize,

    /// Number of times to run each query for averaging
    #[arg(short, long, default_value_t = 5)]
    iterations: usize,
}

// Data structures for log records
#[derive(Debug, Clone, Serialize, Deserialize)]
struct LogRecord {
    doc_id: i64,
    timestamp: String,
    level: String,
    message: String,
    source: LogSource,
    user: User,
    payload_size: u32,
    tags: Vec<String>,
    answers: Vec<Answer>,
    processed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LogSource {
    ip: String,
    host: String,
    region: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct User {
    id: String,
    session_id: String,
    metrics: UserMetrics,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UserMetrics {
    login_time_ms: u32,
    clicks: u32,
    active: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Answer {
    nx_domain: bool,
    response_time_ms: u32,
}

// Aggregation Index Tree structures
#[derive(Debug, Clone)]
struct AggregationIndexTree {
    nodes: Vec<AggregationTreeNode>,
    // Map from original doc_id to position in the tree's sorted values
    doc_id_map: HashMap<u32, usize>,
}

#[derive(Debug, Clone)]
enum AggregationTreeNode {
    Internal {
        split_value: f64,
        left: usize,
        right: usize,
        aggregations: NodeAggregations,
    },
    Leaf {
        doc_ids: Vec<u32>,
        values: Vec<f64>,
        aggregations: NodeAggregations,
    },
}

#[derive(Debug, Clone)]
struct NodeAggregations {
    min_value: f64,
    max_value: f64,
    sum: f64,
    count: u32,
}

impl NodeAggregations {
    fn empty() -> Self {
        NodeAggregations {
            min_value: f64::MAX,
            max_value: f64::MIN,
            sum: 0.0,
            count: 0,
        }
    }

    fn combine(a: &NodeAggregations, b: &NodeAggregations) -> NodeAggregations {
        if a.count == 0 {
            return b.clone();
        }
        if b.count == 0 {
            return a.clone();
        }

        NodeAggregations {
            min_value: a.min_value.min(b.min_value),
            max_value: a.max_value.max(b.max_value),
            sum: a.sum + b.sum,
            count: a.count + b.count,
        }
    }
}

// Traditional columnar storage for comparison
#[derive(Debug, Clone)]
struct ColumnarStorage {
    values: Vec<f64>,
}

// Memory usage tracking
impl DynamicUsage for AggregationIndexTree {
    fn dynamic_usage(&self) -> usize {
        let mut size = 0;
        for node in &self.nodes {
            size += match node {
                AggregationTreeNode::Internal { .. } => std::mem::size_of::<AggregationTreeNode>(),
                AggregationTreeNode::Leaf { doc_ids, values, .. } => {
                    std::mem::size_of::<AggregationTreeNode>() + 
                    doc_ids.capacity() * std::mem::size_of::<u32>() +
                    values.capacity() * std::mem::size_of::<f64>()
                }
            };
        }
        // Add size of doc_id_map
        size += std::mem::size_of::<HashMap<u32, usize>>() + 
                self.doc_id_map.capacity() * (std::mem::size_of::<u32>() + std::mem::size_of::<usize>());
        size
    }

    fn dynamic_usage_bounds(&self) -> (usize, Option<usize>) {
        // Provide a simple implementation for bounds
        (self.dynamic_usage(), Some(self.dynamic_usage()))
    }
}

impl DynamicUsage for ColumnarStorage {
    fn dynamic_usage(&self) -> usize {
        std::mem::size_of::<ColumnarStorage>() + 
        self.values.capacity() * std::mem::size_of::<f64>()
    }

    fn dynamic_usage_bounds(&self) -> (usize, Option<usize>) {
        // Provide a simple implementation for bounds
        (self.dynamic_usage(), Some(self.dynamic_usage()))
    }
}

// Generate random log records
fn generate_random_log_record(i: usize, base_time: DateTime<Utc>) -> LogRecord {
    let mut rng = rand::thread_rng();
    let levels = ["info", "warn", "error", "debug", "trace"];
    let regions = [
        "us-east-1",
        "eu-west-1",
        "eu-west-2",
        "ap-south-1",
        "us-west-2",
    ];
    let hosts = (1..=20)
        .map(|n| format!("server-{}.region.local", n))
        .collect::<Vec<_>>();
    let offset_ms = rng.gen_range(-30000..30000);
    let timestamp = base_time + chrono::Duration::milliseconds(offset_ms);
    let answers_len = rng.gen_range(0..=3);
    let answers = (0..answers_len)
        .map(|_| Answer {
            nx_domain: rng.gen_bool(0.3),
            response_time_ms: rng.gen_range(5..150),
        })
        .collect::<Vec<_>>();
    LogRecord {
        doc_id: i as i64,
        timestamp: timestamp.to_rfc3339(),
        level: levels[rng.gen_range(0..levels.len())].to_string(),
        message: format!("Log message {} for record {}", Uuid::new_v4(), i),
        source: LogSource {
            ip: format!("10.0.{}.{}", rng.gen_range(1..255), rng.gen_range(1..255)),
            host: hosts[rng.gen_range(0..hosts.len())].clone(),
            region: regions[rng.gen_range(0..regions.len())].to_string(),
        },
        user: User {
            id: format!("user_{}", rng.gen_range(1000..50000)),
            session_id: Uuid::new_v4().to_string(),
            metrics: UserMetrics {
                login_time_ms: rng.gen_range(10..1500),
                clicks: rng.gen_range(0..100),
                active: rng.gen_bool(0.75),
            },
        },
        payload_size: rng.gen_range(50..20_480),
        // Generate fewer unique tags for better dictionary encoding demo
        tags: (0..rng.gen_range(1..8))
            .map(|_| format!("tag_{}", rng.gen_range(1..50))) // Keep original tag generation
            .collect::<Vec<_>>(),
        answers,
        processed: rng.gen_bool(0.9),
    }
}

// Build Aggregation Index Tree
fn build_aggregation_index_tree(values: &[(u32, f64)], leaf_size: usize) -> AggregationIndexTree {
    // Create a mapping from original doc_id to position in sorted array
    let mut doc_id_map = HashMap::with_capacity(values.len());
    for (i, &(doc_id, _)) in values.iter().enumerate() {
        doc_id_map.insert(doc_id, i);
    }
    
    let mut nodes = Vec::new();
    // Make sure the root is index 0 by building the tree from index 0
    build_tree_recursive(&mut nodes, values, 0, values.len(), leaf_size);
    
    AggregationIndexTree { 
        nodes,
        doc_id_map,
    }
}

fn build_tree_recursive(
    nodes: &mut Vec<AggregationTreeNode>,
    values: &[(u32, f64)],
    start: usize,
    end: usize,
    leaf_size: usize,
) -> usize {
    let current_idx = nodes.len(); // Save the current index before adding the new node
    
    if end - start <= leaf_size {
        // Create leaf node
        let mut min_value = f64::MAX;
        let mut max_value = f64::MIN;
        let mut sum = 0.0;
        let count = (end - start) as u32;
        
        let mut leaf_doc_ids = Vec::with_capacity(end - start);
        let mut leaf_values = Vec::with_capacity(end - start);
        
        for i in start..end {
            let (doc_id, value) = values[i];
            leaf_doc_ids.push(doc_id);
            leaf_values.push(value);
            
            min_value = min_value.min(value);
            max_value = max_value.max(value);
            sum += value;
        }
        
        let node = AggregationTreeNode::Leaf {
            doc_ids: leaf_doc_ids,
            values: leaf_values,
            aggregations: NodeAggregations {
                min_value,
                max_value,
                sum,
                count,
            },
        };
        
        nodes.push(node);
    } else {
        // Create internal node
        let mid = start + (end - start) / 2;
        let split_value = values[mid].1;
        
        // First add a placeholder for this node to preserve the index
        nodes.push(AggregationTreeNode::Leaf {
            doc_ids: Vec::new(),
            values: Vec::new(),
            aggregations: NodeAggregations::empty(),
        });
        
        let left_idx = build_tree_recursive(nodes, values, start, mid, leaf_size);
        let right_idx = build_tree_recursive(nodes, values, mid, end, leaf_size);
        
        // Get aggregations from children
        let left_aggs = match &nodes[left_idx] {
            AggregationTreeNode::Internal { aggregations, .. } => aggregations,
            AggregationTreeNode::Leaf { aggregations, .. } => aggregations,
        };
        
        let right_aggs = match &nodes[right_idx] {
            AggregationTreeNode::Internal { aggregations, .. } => aggregations,
            AggregationTreeNode::Leaf { aggregations, .. } => aggregations,
        };
        
        // Replace the placeholder with real internal node
        nodes[current_idx] = AggregationTreeNode::Internal {
            split_value,
            left: left_idx,
            right: right_idx,
            aggregations: NodeAggregations {
                min_value: left_aggs.min_value.min(right_aggs.min_value),
                max_value: left_aggs.max_value.max(right_aggs.max_value),
                sum: left_aggs.sum + right_aggs.sum,
                count: left_aggs.count + right_aggs.count,
            },
        };
    }
    
    current_idx
}

// Query functions for AIT
impl AggregationIndexTree {
    fn get_global_aggregations(&self) -> NodeAggregations {
        if self.nodes.is_empty() {
            return NodeAggregations::empty();
        }
        
        match &self.nodes[0] {
            AggregationTreeNode::Internal { aggregations, .. } => aggregations.clone(),
            AggregationTreeNode::Leaf { aggregations, .. } => aggregations.clone(),
        }
    }
    
    fn query_with_bitmap(&self, bitmap: &RoaringBitmap) -> NodeAggregations {
        if self.nodes.is_empty() {
            return NodeAggregations::empty();
        }
        
        // Direct lookup for all bitmaps
        let mut result = NodeAggregations::empty();
        
        for doc_id in bitmap.iter() {
            // Look up the position in the sorted array
            if let Some(&pos) = self.doc_id_map.get(&doc_id) {
                let value = self.get_value_at_position(pos);
                
                // Update aggregations
                if result.count == 0 {
                    result.min_value = value;
                    result.max_value = value;
                } else {
                    result.min_value = result.min_value.min(value);
                    result.max_value = result.max_value.max(value);
                }
                result.sum += value;
                result.count += 1;
            }
        }
        
        result
    }
    
    // Helper method to find a value at a given position in the sorted array
    fn get_value_at_position(&self, pos: usize) -> f64 {
        // Start from the root node and traverse to find the right leaf
        self.find_value_recursive(0, pos)
    }

    fn find_value_recursive(&self, node_idx: usize, global_pos: usize) -> f64 {
        match &self.nodes[node_idx] {
            AggregationTreeNode::Internal { left, right, .. } => {
                // Get the count of elements in the left subtree
                let left_node = &self.nodes[*left];
                let left_count = match left_node {
                    AggregationTreeNode::Internal { aggregations, .. } => aggregations.count as usize,
                    AggregationTreeNode::Leaf { values, .. } => values.len(),
                };
                
                // Determine if the position is in the left or right subtree
                if global_pos < left_count {
                    // Position is in left subtree
                    self.find_value_recursive(*left, global_pos)
                } else {
                    // Position is in right subtree, adjust the position relative to right subtree
                    self.find_value_recursive(*right, global_pos - left_count)
                }
            },
            AggregationTreeNode::Leaf { values, .. } => {
                // We should find the value directly in this leaf node
                values[global_pos]
            }
        }
    }
}

// Traditional aggregation functions for comparison
impl ColumnarStorage {
    fn get_global_aggregations(&self) -> NodeAggregations {
        if self.values.is_empty() {
            return NodeAggregations::empty();
        }
        
        let mut min_value = f64::MAX;
        let mut max_value = f64::MIN;
        let mut sum = 0.0;
        
        for &value in &self.values {
            min_value = min_value.min(value);
            max_value = max_value.max(value);
            sum += value;
        }
        
        NodeAggregations {
            min_value,
            max_value,
            sum,
            count: self.values.len() as u32,
        }
    }
    
    fn query_with_bitmap(&self, bitmap: &RoaringBitmap) -> NodeAggregations {
        let mut result = NodeAggregations::empty();
        
        for (doc_id, &value) in self.values.iter().enumerate() {
            if bitmap.contains(doc_id as u32) {
                if result.count == 0 {
                    result.min_value = value;
                    result.max_value = value;
                } else {
                    result.min_value = result.min_value.min(value);
                    result.max_value = result.max_value.max(value);
                }
                result.sum += value;
                result.count += 1;
            }
        }
        
        result
    }
}

// Benchmark functions
fn run_benchmark(args: &Args) {
    println!("Generating {} random documents...", args.num_docs);
    let base_time = Utc::now();
    
    // Generate documents
    let start = Instant::now();
    let docs: Vec<LogRecord> = (0..args.num_docs)
        .map(|i| generate_random_log_record(i, base_time))
        .collect();
    let generation_time = start.elapsed();
    println!("Document generation time: {:?}", generation_time);
    
    // Extract payload_size values
    println!("Extracting payload_size values...");
    let start = Instant::now();
    let mut values: Vec<(u32, f64)> = docs
        .iter()
        .enumerate()
        .map(|(i, doc)| (i as u32, doc.payload_size as f64))
        .collect();
    let extraction_time = start.elapsed();
    println!("Value extraction time: {:?}", extraction_time);
    
    // Sort values for AIT construction
    println!("Sorting values for AIT construction...");
    let start = Instant::now();
    values.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
    let sorting_time = start.elapsed();
    println!("Value sorting time: {:?}", sorting_time);
    
    // Build AIT
    println!("Building Aggregation Index Tree...");
    let start = Instant::now();
    let ait = build_aggregation_index_tree(&values, args.leaf_size);
    let ait_build_time = start.elapsed();
    println!("AIT build time: {:?}", ait_build_time);
    
    // Build traditional columnar storage
    println!("Building traditional columnar storage...");
    let start = Instant::now();
    let columnar = ColumnarStorage {
        values: docs.iter().map(|doc| doc.payload_size as f64).collect(),
    };
    let columnar_build_time = start.elapsed();
    println!("Columnar storage build time: {:?}", columnar_build_time);
    
    // Generate random document IDs for filtered query
    println!("Generating random document IDs for filtered query...");
    let mut rng = rand::thread_rng();
    let filter_count = (args.num_docs * args.filter_percentage) / 100;
    let mut filter_bitmap = RoaringBitmap::new();
    while filter_bitmap.len() < filter_count as u64 {
        filter_bitmap.insert(rng.gen_range(0..args.num_docs as u32));
    }
    
    // Memory usage
    let ait_memory = ait.dynamic_usage();
    let columnar_memory = columnar.dynamic_usage();
    println!("\nMemory Usage:");
    println!("AIT: {} bytes ({:.2} MB)", ait_memory, ait_memory as f64 / 1_048_576.0);
    println!("Columnar: {} bytes ({:.2} MB)", columnar_memory, columnar_memory as f64 / 1_048_576.0);
    println!("Ratio: {:.2}x", ait_memory as f64 / columnar_memory as f64);
    
    // Benchmark global aggregations
    println!("\nBenchmarking global aggregations...");
    let mut ait_global_times = Vec::with_capacity(args.iterations);
    let mut columnar_global_times = Vec::with_capacity(args.iterations);
    
    for i in 0..args.iterations {
        // AIT global query
        let start = Instant::now();
        let ait_result = ait.get_global_aggregations();
        let ait_time = start.elapsed();
        ait_global_times.push(ait_time);
        
        // Columnar global query
        let start = Instant::now();
        let columnar_result = columnar.get_global_aggregations();
        let columnar_time = start.elapsed();
        columnar_global_times.push(columnar_time);
        
        // Verify results match
        if i == 0 {
            // Print both results for debugging
            println!("AIT min: {}, Columnar min: {}", ait_result.min_value, columnar_result.min_value);
            println!("AIT max: {}, Columnar max: {}", ait_result.max_value, columnar_result.max_value);
            
            // Use approximate equality for floating point comparisons
            assert!((ait_result.min_value - columnar_result.min_value).abs() < 0.001, 
                   "Min values don't match: AIT={}, Columnar={}", 
                   ait_result.min_value, columnar_result.min_value);
            assert!((ait_result.max_value - columnar_result.max_value).abs() < 0.001,
                   "Max values don't match: AIT={}, Columnar={}", 
                   ait_result.max_value, columnar_result.max_value);
            assert!((ait_result.sum - columnar_result.sum).abs() < 0.001,
                   "Sum values don't match: AIT={}, Columnar={}", 
                   ait_result.sum, columnar_result.sum);
            assert_eq!(ait_result.count, columnar_result.count,
                      "Count values don't match: AIT={}, Columnar={}", 
                      ait_result.count, columnar_result.count);
            
            println!("Global aggregation results:");
            println!("  Min: {}", ait_result.min_value);
            println!("  Max: {}", ait_result.max_value);
            println!("  Sum: {}", ait_result.sum);
            println!("  Count: {}", ait_result.count);
            println!("  Avg: {}", ait_result.sum / ait_result.count as f64);
        }
    }
    
    // Benchmark filtered aggregations
    println!("\nBenchmarking filtered aggregations ({} documents, {}%)...", 
             filter_bitmap.len(), args.filter_percentage);
    let mut ait_filtered_times = Vec::with_capacity(args.iterations);
    let mut columnar_filtered_times = Vec::with_capacity(args.iterations);
    
    for i in 0..args.iterations {
        // AIT filtered query
        let start = Instant::now();
        let ait_result = ait.query_with_bitmap(&filter_bitmap);
        let ait_time = start.elapsed();
        ait_filtered_times.push(ait_time);
        
        // Columnar filtered query
        let start = Instant::now();
        let columnar_result = columnar.query_with_bitmap(&filter_bitmap);
        let columnar_time = start.elapsed();
        columnar_filtered_times.push(columnar_time);
        
        // Verify results match
        if i == 0 {
            // Print both results for debugging
            println!("AIT min: {}, Columnar min: {}", ait_result.min_value, columnar_result.min_value);
            println!("AIT max: {}, Columnar max: {}", ait_result.max_value, columnar_result.max_value);
            
            // Use approximate equality for floating point comparisons
            assert!((ait_result.min_value - columnar_result.min_value).abs() < 0.001, 
                   "Min values don't match: AIT={}, Columnar={}", 
                   ait_result.min_value, columnar_result.min_value);
            assert!((ait_result.max_value - columnar_result.max_value).abs() < 0.001,
                   "Max values don't match: AIT={}, Columnar={}", 
                   ait_result.max_value, columnar_result.max_value);
            assert!((ait_result.sum - columnar_result.sum).abs() < 0.001,
                   "Sum values don't match: AIT={}, Columnar={}", 
                   ait_result.sum, columnar_result.sum);
            assert_eq!(ait_result.count, columnar_result.count,
                      "Count values don't match: AIT={}, Columnar={}", 
                      ait_result.count, columnar_result.count);
            
            println!("Filtered aggregation results:");
            println!("  Min: {}", ait_result.min_value);
            println!("  Max: {}", ait_result.max_value);
            println!("  Sum: {}", ait_result.sum);
            println!("  Count: {}", ait_result.count);
            println!("  Avg: {}", ait_result.sum / ait_result.count as f64);
        }
    }
    
    // Calculate and report average times
    let avg_ait_global = average_duration(&ait_global_times);
    let avg_columnar_global = average_duration(&columnar_global_times);
    let avg_ait_filtered = average_duration(&ait_filtered_times);
    let avg_columnar_filtered = average_duration(&columnar_filtered_times);
    
    println!("\nPerformance Results (averaged over {} iterations):", args.iterations);
    println!("Global Aggregations:");
    println!("  AIT: {:?}", avg_ait_global);
    println!("  Columnar: {:?}", avg_columnar_global);
    println!("  Speedup: {:.2}x", avg_columnar_global.as_nanos() as f64 / avg_ait_global.as_nanos() as f64);
    
    println!("\nFiltered Aggregations:");
    println!("  AIT: {:?}", avg_ait_filtered);
    println!("  Columnar: {:?}", avg_columnar_filtered);
    println!("  Speedup: {:.2}x", avg_columnar_filtered.as_nanos() as f64 / avg_ait_filtered.as_nanos() as f64);
    
    println!("\nSummary:");
    println!("- AIT build time: {:?}", ait_build_time);
    println!("- AIT memory overhead: {:.2}x", ait_memory as f64 / columnar_memory as f64);
    println!("- Global query speedup: {:.2}x", avg_columnar_global.as_nanos() as f64 / avg_ait_global.as_nanos() as f64);
    println!("- Filtered query speedup: {:.2}x", avg_columnar_filtered.as_nanos() as f64 / avg_ait_filtered.as_nanos() as f64);
}

fn average_duration(durations: &[Duration]) -> Duration {
    let total_nanos: u128 = durations.iter().map(|d| d.as_nanos()).sum();
    Duration::from_nanos((total_nanos / durations.len() as u128) as u64)
}

fn main() {
    let args = Args::parse();
    println!("AIT Benchmark");
    println!("=============");
    println!("Configuration:");
    println!("- Number of documents: {}", args.num_docs);
    println!("- Filter percentage: {}%", args.filter_percentage);
    println!("- Leaf size: {}", args.leaf_size);
    println!("- Iterations: {}", args.iterations);
    println!();
    
    run_benchmark(&args);
}

