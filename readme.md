# 🐒 MonkeyDB

A minimal, Pythonic datastore solution with file-backed persistence and structured record operations.  
*Ideal for embedded use cases, mock layer testing, and scenarios where full RDBMS overhead is suboptimal.*

---

## 🧠 Abstract

MonkeyDB implements a constrained transactional interface inspired by subset-SQL semantics using a JSON-backed I/O buffer.  
It’s intentionally constrained in features, yet optionally extensible via Python-native types.

---

## ✨ Features

- `CREATE TABLE`-style in-memory declarations
- `INSERT` operations with schema validation
- `SELECT` queries with basic condition filters
- Flat-file persistence via atomic JSON serialization
- Internal indexing through record scan optimization (O(n) 😎)
- Optional integrity enforcement (schema-bound keys)
- Stateless initialization (zero-config)
- Minimal runtime footprint (~single file)

---

## 📦 Installation
> Note: MonkeyDB is a zero-dependency artifact, installable via any modern Python environment.

```bash
pip install monkeydb
```
