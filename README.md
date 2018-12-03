# SQLiteMini.playground
One-file Swift SQLite library.

Copy and paste [SQLiteDatabase.swift](https://github.com/kustra/SQLiteMini.playground/blob/master/Sources/SQLiteDatabase.swift) into your project and start using it!
Note that for most use cases, you'd want to use a more robust library like [GRDB.swift](https://github.com/groue/GRDB.swift) or [SQLite.swift](https://github.com/stephencelis/SQLite.swift). This library is intended for those who don't want to use an external dependency for their project.

Example:
```swift
let db = try SQLiteDatabase(path: "path/to/database.sqlite")

if try !db.tableExists("test") {
    try db.execute(sql: "CREATE TABLE test (col TEXT NOT NULL, i INT NOT NULL, n INT)")
}

try db.prepare(sql: "INSERT INTO test (col, i, n) VALUES (:x, :i, :n)")
    .bind(":x", "Test content")
    .bind(":i", 42 as Int32)
    .bind(":n", nil as Int32?)
    .execute()

try db.prepare(sql: "SELECT * FROM test WHERE col LIKE :a")
    .bind(":a", "T%")
    .query { row in
        try print("\(row.text("col")), \(row.int("i")), \(String(describing: row.optionalInt("n")))")
    }
```
