use std::collections::HashMap;

use futures::{StreamExt, TryStreamExt};
use iceberg::{table, Catalog, NamespaceIdent, Result};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};

fn setup_env() {
    std::env::set_var("REST_CATALOG_ENDPOINT", "http://10.246.102.158:32165");
    std::env::set_var(
        "OSS_ENDPOINT",
        "http://oss0c83-cn-baoding-gwmcloud-d01-a.ops.cloud.gwm.cn",
    );
    std::env::set_var("OSS_ACCESS_KEY_ID", "aQqxpzF3o2hjSH9b");
    std::env::set_var("OSS_ACCESS_KEY_SECRET", "KcgH0qHnKcXIaOIzqNRxnlwQReGvMa");
}

#[tokio::main]
async fn main() -> Result<()> {
    setup_env();
    // Build your file IO.
    // Connect to a catalog.
    let catalog = connect_rest_catalog(
        std::env::var("REST_CATALOG_ENDPOINT")
            .expect("REST_CATALOG_ENDPOINT environment variable not set"),
        "oss://multimodal".into(),
        std::env::var("OSS_ENDPOINT").expect("OSS_ENDPOINT environment variable not set"),
        std::env::var("OSS_ACCESS_KEY_ID").expect("OSS_ENDPOINT environment variable not set"),
        std::env::var("OSS_ACCESS_KEY_SECRET").expect("OSS_ENDPOINT environment variable not set"),
    );
    // Load table from catalog.
    let namespaces = catalog.list_namespaces(None).await?;
    for namespace in namespaces {
        println!("namespace: {:?}", namespace);
        let tables = catalog.list_tables(&namespace).await?;
        for table in tables {
            println!("\ttable: {:?}", table);
            let t = catalog.load_table(&table).await?;
            if t.metadata().current_snapshot().is_none() {
                continue;
            }
            if table
                .namespace
                .ne(&NamespaceIdent::from_strs(&["t2_db"]).unwrap())
            {
                continue;
            }
            if table.name().ne("my_test_table") {
                continue;
            }
            let fields = t.metadata().current_schema().as_struct().fields();
            println!("\tfields: {}", fields.len());
            println!("\t\t{:?}", fields);
            if fields.is_empty() {
                continue;
            }
            let mut fields = fields.to_vec();
            fields.sort_by_key(|field| field.id);
            let names = fields
                .iter()
                .map(|filed| filed.name.clone())
                .collect::<Vec<_>>();
            println!("\t\tnames: {:?}", names);
            let stream = t
                .scan()
                .select(&names)
                .build()
                .expect("build select")
                // .plan_files()
                // .await
                // .expect("plan files")
                .to_arrow()
                .await
                .expect("to arrow await")
                .take(10);
            // Consume this stream like arrow record batch stream.
            let data: Vec<_> = stream.try_collect().await?;
            for item in data {
                println!("\t\trecords: {:?}", item["id"]);
            }
            break;
        }
    }

    Ok(())
}

fn connect_rest_catalog(
    uri: String,
    warehouse: String,
    endpoint: String,
    access_key: String,
    secret_key: String,
) -> RestCatalog {
    RestCatalog::new(
        RestCatalogConfig::builder()
            .uri(uri)
            .warehouse(warehouse)
            .props(HashMap::<String, String>::from_iter([
                (String::from(iceberg::io::OSS_ENDPOINT), endpoint),
                (String::from(iceberg::io::OSS_ACCESS_KEY_ID), access_key),
                (String::from(iceberg::io::OSS_ACCESS_KEY_SECRET), secret_key),
            ]))
            .build(),
    )
}
