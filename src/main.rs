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
            /*
            /// larger graph with all 5 columns
            edges.push(Edge {
                to: NodeID(array[1] as u32),
                cost: Cost(array[0] as u16),
                additional1: Additional(array[2] as u16),
                additional2: Additional(array[3] as u16),
                additional3: Additional(array[4] as u16),
            });
            */
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

    // read from GCS

    Ok(())
}
