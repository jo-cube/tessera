//! Two related keyspaces: orders and an index of orders by customer, kept in
//! step by cross-keyspace batches, read through one consistent snapshot, then
//! checkpointed and reopened.

use tessera::{Engine, Keyspace, Options, WriteBatch};

/// Adds an order and its index entry in one atomic batch.
fn place(
    engine: &Engine,
    orders: &Keyspace,
    by_customer: &Keyspace,
    id: u32,
    customer: &str,
    total: u32,
) -> tessera::Result<()> {
    let order = format!("order/{id:06}");
    let mut batch = WriteBatch::new();
    batch.put(
        orders,
        order.as_bytes(),
        format!("{customer},{total}").as_bytes(),
    );
    batch.put(by_customer, format!("{customer}/{order}").as_bytes(), b"");
    engine.write(&batch)
}

/// Sums one customer's orders through `snapshot`. The index and the orders
/// come from the same moment, so every index entry finds its order.
fn customer_total(
    snapshot: &tessera::Snapshot,
    orders: &Keyspace,
    by_customer: &Keyspace,
    customer: &str,
) -> tessera::Result<(usize, u64)> {
    let mut index = snapshot.reader();
    let mut values = snapshot.reader();
    let mut scan = index.prefix(by_customer, format!("{customer}/").as_bytes());
    let (mut count, mut total) = (0, 0);
    while let Some((key, _)) = scan.next()? {
        let order = &key[customer.len() + 1..];
        let value = values.get(orders, order)?.expect("indexed order exists");
        let value = std::str::from_utf8(value).expect("UTF-8 order");
        total += value.rsplit(',').next().unwrap().parse::<u64>().unwrap();
        count += 1;
    }
    Ok((count, total))
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = std::env::temp_dir().join(format!("tessera-keyspaces-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);

    let engine = Engine::open(&dir, Options::default())?;
    let orders = engine.keyspace("orders")?;
    let by_customer = engine.keyspace("orders.by-customer")?;
    for id in 0..1000 {
        let customer = ["ada", "grace", "katherine"][id as usize % 3];
        place(&engine, &orders, &by_customer, id, customer, id % 50)?;
    }

    let ada = (0..1000u64).filter(|id| id % 3 == 0).map(|id| id % 50);
    let ada = (ada.clone().count(), ada.sum::<u64>());

    // The snapshot is taken before the next write, which it does not see.
    let snapshot = engine.snapshot();
    place(&engine, &orders, &by_customer, 1000, "ada", 49)?;
    assert_eq!(
        customer_total(&snapshot, &orders, &by_customer, "ada")?,
        ada
    );
    let latest = customer_total(&engine.snapshot(), &orders, &by_customer, "ada")?;
    assert_eq!(latest, (ada.0 + 1, ada.1 + 49));
    drop(snapshot);

    // Moving an order to another customer touches both keyspaces at once.
    let mut batch = WriteBatch::new();
    batch.put(&orders, b"order/000000", b"grace,0");
    batch.delete(&by_customer, b"ada/order/000000");
    batch.put(&by_customer, b"grace/order/000000", b"");
    engine.write(&batch)?;

    engine.checkpoint()?;
    engine.close()?;

    let engine = Engine::open(&dir, Options::default())?;
    assert_eq!(engine.keyspaces(), ["orders", "orders.by-customer"]);
    let orders = engine.keyspace("orders")?;
    let by_customer = engine.keyspace("orders.by-customer")?;
    let snapshot = engine.snapshot();
    // Order 0 had a total of 0, so only the counts move.
    assert_eq!(
        customer_total(&snapshot, &orders, &by_customer, "ada")?,
        (ada.0, ada.1 + 49)
    );
    let grace = customer_total(&snapshot, &orders, &by_customer, "grace")?;
    assert_eq!(grace.0, 334);
    drop(snapshot);
    engine.close()?;
    std::fs::remove_dir_all(&dir)?;
    println!("keyspaces: consistent across batches, snapshots and reopen");
    Ok(())
}
