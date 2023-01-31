use google_cloud_storage::client::Client;
use google_cloud_storage::http::objects::download::Range;
use google_cloud_storage::http::objects::get::GetObjectRequest;
use google_cloud_storage::http::objects::upload::UploadObjectRequest;
use google_cloud_storage::http::Error;
use google_cloud_storage::sign::SignedURLMethod;
use google_cloud_storage::sign::SignedURLOptions;
use nanorand::{Rng, WyRand};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use std::collections::{BinaryHeap, HashMap};
use std::fs::File;
use std::io::Read;
use std::io::{BufReader, BufWriter};
use std::str;
use std::time::Instant;
use tokio::task::JoinHandle;
use std::thread;
use std::time::Duration;
use std::sync::Mutex;
use self::priority_queue::PriorityQueueItem;

extern crate num_cpus;

mod priority_queue;



fn print_type_of<T>(_: &T) {
    println!("{}", std::any::type_name::<T>())
}

#[derive(Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy, Debug)]
struct NodeID(u32);

// Seconds
#[derive(Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Debug)]
struct Cost(u16);

/// including all 5 columns of arrays so data included is comparible with python runs
#[derive(Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Debug)]
struct Additional(u16);

#[derive(Serialize, Deserialize)]
struct Edge {
    to: NodeID,
    cost: Cost,
    additional1: Additional,
    additional2: Additional,
    additional3: Additional,
}

#[derive(Serialize, Deserialize)]
struct EdgeOriginal {
    to: NodeID,
    cost: Cost,
}

#[derive(Serialize, Deserialize)]
struct Graph {
    // Index is NodeID
    edges_per_node: Vec<SmallVec<[EdgeOriginal; 4]>>,
}


// experimental mutex graph: to write to from multiple workers
#[derive(Serialize, Deserialize)]
pub struct MutexGraph {
    edges_per_node: Mutex<Vec<SmallVec<[EdgeOriginal; 4]>>>
}



fn floodfill(graph: &Graph, start: NodeID) -> HashMap<NodeID, Cost> {
    let time_limit = Cost(3600);

    let mut queue: BinaryHeap<PriorityQueueItem<Cost, NodeID>> = BinaryHeap::new();
    queue.push(PriorityQueueItem {
        cost: Cost(0),
        value: start,
    });

    let mut cost_per_node = HashMap::new();

    while let Some(current) = queue.pop() {
        if cost_per_node.contains_key(&current.value) {
            continue;
        }
        if current.cost > time_limit {
            continue;
        }
        cost_per_node.insert(current.value, current.cost);

        for edge in &graph.edges_per_node[current.value.0 as usize] {
            queue.push(PriorityQueueItem {
                cost: Cost(current.cost.0 + edge.cost.0),
                value: edge.to,
            });
        }
    }

    cost_per_node
}



#[tokio::main]
async fn main() -> Result<(), Error> {
 
   
    // Create client.
    let mut client = Client::default().await.unwrap();

    
    // Download the file from GCS
    let now = Instant::now();
    let data = client
        .download_object(
            &GetObjectRequest {
                bucket: "hack-bucket-8204707942".to_string(),
                object: "walk_network_full.json".to_string(),
                ..Default::default()
            },
            &Range::default(),
            None,
        )
        .await;
    println!("Loading took {:?}", now.elapsed());
    print_type_of(&data);
    

    // convert to text
    let now = Instant::now();
    let text = match data {
        Ok(bytes) => match str::from_utf8(&bytes) {
            Ok(s) => s.to_owned(),
            Err(e) => {
                println!("Error converting bytes to string: {:?}", e);
                "".to_owned()
            }
        },
        Err(e) => {
            println!("Error downloading object: {:?}", e);
            "".to_owned()
        }
    };
    println!("Loading a string {:?}", now.elapsed());
    

    /// comment out the above and uncomment this to read from local file
    /*
    let now = Instant::now();
    let text = std::fs::read_to_string("walk.txt").unwrap();
    println!("Loading a string {:?}", now.elapsed());
    */
    
    
    //// uncomment this section to load from string and save file
    
    print_type_of(&text);
    let first_50 = text.chars().take(50).collect::<String>();
    println!("First 50 characters: {}", first_50);

    // load as hashmap: this takes nearly a minute
    let now = Instant::now();
    let input: HashMap<String, Vec<[usize; 5]>> = serde_json::from_str(&text).unwrap();
    println!("Loaded serde_json {:?}", now.elapsed());
    println!("input length: {:?}", input.len());

    // convert to buffer
    println!("Converting into graph");
    let now = Instant::now();
    let mut graph = Graph {
        edges_per_node: std::iter::repeat_with(SmallVec::new)
            .take(9739277)
            .collect(),
    };
    for (from, input_edges) in input {
        let mut edges = SmallVec::new();
        for array in input_edges {
            /// larger graph with all 5 columns
            //edges.push(Edge {
             //   to: NodeID(array[1] as u32),
             //   cost: Cost(array[0] as u16),
             //   additional1: Additional(array[2] as u16),
             //   additional2: Additional(array[3] as u16),
             //   additional3: Additional(array[4] as u16),
            //});

        edges.push(EdgeOriginal {
                to: NodeID(array[1] as u32),
                cost: Cost(array[0] as u16),
            });
        }

        let from: usize = from.parse().unwrap();
        graph.edges_per_node[from] = edges;
    }
    println!("Converted to graph {:?}", now.elapsed());

    // save buffer: could use this when making the docker image too
    println!("Saving the graph");
    let now = Instant::now();
    let file = BufWriter::new(File::create("graph.bin").unwrap());
    bincode::serialize_into(file, &graph).unwrap();
    println!("Saved to local storage {:?}", now.elapsed());

    
    
    ////Read in local file. 15s for Edge; 10s for EdgeOriginal
    println!("Loading graph");
    let now = Instant::now();
    let file = BufReader::new(File::open("graph.bin").unwrap());
    let graph: Graph = bincode::deserialize_from(file).unwrap();
    println!("Loading took {:?}", now.elapsed());
    
    
    // upload to GCS
    let now = Instant::now();
    let uploaded = client.upload_object(&UploadObjectRequest {
        bucket: "hack-bucket-8204707942".to_string(),
        name: "graph.bin".to_string(),
        ..Default::default()
    },&bincode::serialize(&graph).unwrap(), "application/octet-stream", None).await;
    println!("Saving to GCS took {:?}", now.elapsed());
    
    
    
    // read from GCS
    let now = Instant::now();
    let walking_network = client.download_object(&GetObjectRequest {
        bucket: "hack-bucket-8204707942".to_string(),
        object: "graph.bin".to_string(),
        ..Default::default()
   }, &Range::default(), None).await;
    print_type_of(&walking_network);
    println!("Read from GCS took {:?}", now.elapsed());
    
  
    
    /// to format to use
    let match_out = match walking_network {
        Ok(v) => {
            
            let decoded_walking_network: Graph = bincode::deserialize(&v).unwrap();
            println!("Read from GCS and decoding took {:?}", now.elapsed());
                        
            
            //// Benchmark djikstra
            let mut rng = WyRand::new();
            let now = Instant::now();
            let mut x = 0;
            for _ in 0..1000 {
                let start = NodeID(rng.generate_range(0..decoded_walking_network.edges_per_node.len() as u32));
                let results = floodfill(&decoded_walking_network, start);
                x += results.len()
            }
            println!("Calculating routes took {:?}", now.elapsed());
            println!("Total iters {:?}", x);

            
            
            /////////// experimental: writing to a mutex
            /*
            
            // make empty mutex graph to populate
            let now = Instant::now();
            let mut mutex_graph = MutexGraph {
                edges_per_node: Mutex::new(
                    std::iter::repeat_with(SmallVec::new)
                    .take(9739277)
                    .collect()
                ),
            };
            println!("Create empty MutexGraph in {:?}", now.elapsed());
            
            
            
            /// populate mutex_graph
            let mut edges_per_node = mutex_graph.edges_per_node.lock().unwrap();
            let mut x=0;
            for node_edges in decoded_walking_network.edges_per_node.iter() {
                for edge in node_edges.iter() {
                    println!("{:?}", x);
                }
                if x > 100 {
                    break;
                }
                x += 1;
                
                // add to mutex graph
                edges_per_node.push(node_edges);

            }
            */
            
                    
            

            /*
            for (from, input_edges) in input {
                let mut edges = SmallVec::new();
                for array in input_edges {
                    /// larger graph with all 5 columns
                    edges.push(Edge {
                        to: NodeID(array[1] as u32),
                        cost: Cost(array[0] as u16),
                        additional1: Additional(array[2] as u16),
                        additional2: Additional(array[3] as u16),
                        additional3: Additional(array[4] as u16),
                    });

                //edges.push(EdgeOriginal {
                //        to: NodeID(array[1] as u32),
                //        cost: Cost(array[0] as u16),
                //    });
                }

                let from: usize = from.parse().unwrap();
                graph.edges_per_node[from] = edges;
            }
            println!("Converted to graph {:?}", now.elapsed());
            */


            
            
            
        }, 
        Err(e) => println!("error parsing header: {e:?}"),
    };

    
    
    
    
    
    // count logical cores this process could try to use
    let num = num_cpus::get();
    println!("Number of CPUs {:?}", num);
    
    
    // making process with 8 workers
    for worker_id in 1..num+1 {
        println!("hi from worker {} ", worker_id);
    };
    
            
    
        
    // could distribute to multiple files per eighth of data, each worker can oscillate between decoding the data
    // and writing it to the mutex. This would be some kind of function
    
    
    
    

    Ok(())
}
