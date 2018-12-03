import Foundation
import PlaygroundSupport

// Create test database
let fileManager = FileManager.default
let dbFile = fileManager.temporaryDirectory.appendingPathComponent("test.sqlite")
if fileManager.fileExists(atPath: dbFile.path) {
    try fileManager.removeItem(atPath: dbFile.path)
}

let db = try SQLiteDatabase(path: dbFile.absoluteString)

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
