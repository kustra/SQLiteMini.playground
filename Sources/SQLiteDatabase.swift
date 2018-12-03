// MIT License
//
// Copyright (c) 2018 Laszlo Zsolt Kustra
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

import Foundation
import SQLite3

/// Thin SQLite C API wrapper.
public class SQLiteDatabase {
    fileprivate let dbPtr: OpaquePointer

    private init(dbPtr: OpaquePointer) {
        self.dbPtr = dbPtr
    }

    deinit {
        sqlite3_close(dbPtr)
    }

    public convenience init(path: String) throws {
        var dbPtr: OpaquePointer?
        if sqlite3_open(path, &dbPtr) == SQLITE_OK, let dbPtr = dbPtr {
            self.init(dbPtr: dbPtr)
        } else {
            defer {
                if dbPtr != nil {
                    sqlite3_close(dbPtr)
                }
            }

            if let dbPtr = dbPtr, let errorPtr = sqlite3_errmsg(dbPtr) {
                throw SQLiteError.openDatabase(message: String(cString: errorPtr))
            } else {
                throw SQLiteError.openDatabase(message: "No error message provided from sqlite.")
            }
        }
    }

    fileprivate var errorMessage: String {
        if let errorPtr = sqlite3_errmsg(dbPtr) {
            let errorMessage = String(cString: errorPtr)
            return errorMessage
        } else {
            return "No error message provided from sqlite."
        }
    }

    public func prepare(sql: String) throws -> SQLitePreparedStatement {
        return try SQLitePreparedStatement(sql: sql, db: self)
    }
    
    public func execute(sql: String) throws {
        return try prepare(sql: sql).execute()
    }
}

public enum SQLiteError: Error {
    case openDatabase(message: String)
    case prepare(message: String)
    case step(message: String)
    case bind(message: String)
    case result(message: String)
}

public class SQLitePreparedStatement {
    private let db: SQLiteDatabase
    fileprivate let stmtPtr: OpaquePointer

    private init(stmtPtr: OpaquePointer, db: SQLiteDatabase) {
        self.db = db
        self.stmtPtr = stmtPtr
    }

    deinit {
        sqlite3_finalize(stmtPtr)
    }
    
    fileprivate convenience init(sql: String, db: SQLiteDatabase) throws {
        var stmtPtr: OpaquePointer?
        if sqlite3_prepare_v2(db.dbPtr, sql, -1, &stmtPtr, nil) == SQLITE_OK, let stmtPtr = stmtPtr {
            self.init(stmtPtr: stmtPtr, db: db)
        } else {
            throw SQLiteError.prepare(message: db.errorMessage)
        }
    }
}

extension SQLitePreparedStatement {

    private func bind<T>(_ idx: Int32, _ value: T?, _ nonNilBinder: (OpaquePointer, Int32, T) -> Int32) throws {
        if let value = value {
            guard nonNilBinder(stmtPtr, idx, value) == SQLITE_OK else {
                throw SQLiteError.bind(message: db.errorMessage)
            }
        } else {
            guard sqlite3_bind_null(stmtPtr, idx) == SQLITE_OK else {
                throw SQLiteError.bind(message: db.errorMessage)
            }
        }
    }
    
    private func bind<T>(_ name: String, _ value: T?, _ nonNilBinder: (OpaquePointer, Int32, T) -> Int32) throws {
        let nsName = name as NSString
        let idx = sqlite3_bind_parameter_index(stmtPtr, nsName.utf8String)
        guard idx != 0 else {
            throw SQLiteError.bind(message: "Bind parameter '\(name)' not found!")
        }
        try bind(idx, value, nonNilBinder)
    }
    
    public func bind(_ idx: Int32, _ value: Int32?) throws -> SQLitePreparedStatement {
        try bind(idx, value, sqlite3_bind_int)
        return self
    }
    
    public func bind(_ name: String, _ value: Int32?) throws -> SQLitePreparedStatement {
        try bind(name, value, sqlite3_bind_int)
        return self
    }

    public func bind(_ idx: Int32, _ value: Int64?) throws -> SQLitePreparedStatement {
        try bind(idx, value, sqlite3_bind_int64)
        return self
    }

    public func bind(_ name: String, _ value: Int64?) throws -> SQLitePreparedStatement {
        try bind(name, value, sqlite3_bind_int64)
        return self
    }
    
    public func bind(_ idx: Int32, _ value: Double?) throws -> SQLitePreparedStatement {
        try bind(idx, value, sqlite3_bind_double)
        return self
    }
    
    public func bind(_ name: String, _ value: Double?) throws -> SQLitePreparedStatement {
        try bind(name, value, sqlite3_bind_double)
        return self
    }
    
    public func bind(_ idx: Int32, _ value: String?) throws -> SQLitePreparedStatement {
        try bind(idx, value) { stmtPtr, idx, value in
            let nsValue = value as NSString
            return sqlite3_bind_text(stmtPtr, idx, nsValue.utf8String, -1, nil)
        }
        return self
    }
    
    public func bind(_ name: String, _ value: String?) throws -> SQLitePreparedStatement {
        try bind(name, value) { stmtPtr, idx, value in
            let nsValue = value as NSString
            return sqlite3_bind_text(stmtPtr, idx, nsValue.utf8String, -1, nil)
        }
        return self
    }

    public func execute() throws {
        defer {
            sqlite3_reset(stmtPtr)
        }

        guard sqlite3_step(stmtPtr) == SQLITE_DONE else {
            throw SQLiteError.step(message: db.errorMessage)
        }
    }
    
    public func query<T>(parser: (SQLiteRow) throws -> T) rethrows -> [T] {
        defer {
            sqlite3_reset(stmtPtr)
        }

        var result = [T]()
        let row = SQLiteRow(stmt: self)
        while sqlite3_step(stmtPtr) == SQLITE_ROW {
            result.append(try parser(row))
        }

        return result
    }
    
    public func queryEmpty() throws -> Bool {
        defer {
            sqlite3_reset(stmtPtr)
        }

        switch sqlite3_step(stmtPtr) {
        case SQLITE_ROW:
            return false
        case SQLITE_DONE:
            return true
        default:
            throw SQLiteError.step(message: "Unexpected state from step!")
        }
    }
}

public class SQLiteRow {
    private let stmt: SQLitePreparedStatement
    private var columnNames: [String: Int32]!

    fileprivate init(stmt: SQLitePreparedStatement) {
        self.stmt = stmt
    }

    private func columnIndex(for name: String) throws -> Int32 {
        if columnNames == nil {
            var columnNames = [String: Int32]()
            let columnCount = sqlite3_column_count(stmt.stmtPtr)
            for idx in 0..<columnCount {
                guard let cName = sqlite3_column_name(stmt.stmtPtr, idx) else {
                    throw SQLiteError.result(message: "Error while loading column names!")
                }
                columnNames[String(cString: cName)] = idx
            }
            self.columnNames = columnNames
        }

        guard let idx = columnNames[name] else {
            throw SQLiteError.result(message: "Unknown column name: \(name)")
        }

        return idx
    }
    
    private func optional<T>(_ idx: Int32, _ nonNilGetter: (Int32) -> T) -> T? {
        if sqlite3_column_type(stmt.stmtPtr, idx) == SQLITE_NULL {
            return nil
        } else {
            return nonNilGetter(idx)
        }
    }
    
    public func int(_ idx: Int32) -> Int32 {
        return sqlite3_column_int(stmt.stmtPtr, idx)
    }
    
    public func int(_ name: String) throws -> Int32 {
        return try int(columnIndex(for: name))
    }
    
    public func optionalInt(_ idx: Int32) -> Int32? {
        return optional(idx, self.int)
    }

    public func optionalInt(_ name: String) throws -> Int32? {
        return try optionalInt(columnIndex(for: name))
    }
    
    public func int64(_ idx: Int32) -> Int64 {
        return sqlite3_column_int64(stmt.stmtPtr, idx)
    }

    public func int64(_ name: String) throws -> Int64 {
        return try int64(columnIndex(for: name))
    }
    
    public func optionalInt64(_ idx: Int32) -> Int64? {
        return optional(idx, self.int64)
    }
    
    public func optionalInt64(_ name: String) throws -> Int64? {
        return try optionalInt64(columnIndex(for: name))
    }

    public func double(_ idx: Int32) -> Double {
        return sqlite3_column_double(stmt.stmtPtr, idx)
    }
    
    public func double(_ name: String) throws -> Double {
        return try double(columnIndex(for: name))
    }
    
    public func optionalDouble(_ idx: Int32) -> Double? {
        return optional(idx, self.double)
    }
    
    public func optionalDouble(_ name: String) throws -> Double? {
        return try optionalDouble(columnIndex(for: name))
    }

    public func text(_ idx: Int32) -> String {
        let cRes = sqlite3_column_text(stmt.stmtPtr, idx)!
        return String(cString: cRes)
    }
    
    public func text(_ name: String) throws -> String {
        return try text(columnIndex(for: name))
    }
    
    public func optionalText(_ idx: Int32) -> String? {
        return optional(idx, self.text)
    }
    
    public func optionalText(_ name: String) throws -> String? {
        return try optionalText(columnIndex(for: name))
    }
}

extension SQLiteDatabase {
    
    public func exists(type: String, name: String) throws -> Bool {
        // SQlite identifiers are case-insensitive, case-preserving:
        // http://www.alberton.info/dbms_identifiers_and_case_sensitivity.html
        let name = name.lowercased()

        if try prepare(sql: "SELECT 1 FROM sqlite_master WHERE type = :type AND LOWER(name) = :name")
            .bind(":type", type)
            .bind(":name", name)
            .queryEmpty() == false
        { return true }

        if try prepare(sql: "SELECT 1 FROM sqlite_temp_master WHERE type = :type AND LOWER(name) = :name")
            .bind(":type", type)
            .bind(":name", name)
            .queryEmpty() == false
        { return true }
        
        return false
    }

    public func tableExists(_ name: String) throws -> Bool {
        return try exists(type: "table", name: name)
    }
}
