import os
import json
import re
import uuid
import threading
from typing import Dict, List, Any, Union, Optional, Tuple
from datetime import datetime
import hashlib
import queue
import shutil
import tempfile
import time
import gc
import logging
from enum import Enum
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import heapq

class StorageEngine:
    def __init__(self, base_path: str = 'database_storage'):
        """
        Initialize the storage engine with a base storage path

        Args:
            base_path (str): Base directory for storing database files
        """
        self.base_path = base_path
        os.makedirs(base_path, exist_ok=True)
        self.lock = threading.Lock()
        self.cache = {}
        self.cache_size = 1000
        self.cache_hits = 0
        self.cache_misses = 0
        self.transaction_manager = TransactionManager()
        self.backup_manager = BackupManager(base_path)
        self.index_manager = IndexManager(base_path)
        self.logger = logging.getLogger('StorageEngine')
        self.logger.setLevel(logging.INFO)
        self.setup_logging()

    def setup_logging(self):
        """Setup logging configuration"""
        log_file = os.path.join(self.base_path, 'storage.log')
        handler = logging.FileHandler(log_file)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

    def create_table_file(self, table_name: str) -> str:
        """
        Create a new table file with metadata

        Args:
            table_name (str): Name of the table

        Returns:
            str: Path to the created table file
        """
        table_path = os.path.join(self.base_path, f"{table_name}.json")
        metadata_path = os.path.join(self.base_path, f"{table_name}_meta.json")
        
        # Create table file
        with open(table_path, 'w') as f:
            json.dump([], f)
            
        # Create metadata file
        metadata = {
            'created_at': datetime.now().isoformat(),
            'last_modified': datetime.now().isoformat(),
            'indexes': {},
            'constraints': {},
            'statistics': {
                'row_count': 0,
                'last_access': datetime.now().isoformat()
            }
        }
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)
            
        self.logger.info(f"Created table: {table_name}")
        return table_path

    def write_data(self, table_name: str, data: List[Dict[str, Any]], transaction_id: Optional[str] = None):
        """
        Write data to a table file with caching and transaction support

        Args:
            table_name (str): Name of the table
            data (List[Dict]): Data to write
            transaction_id (str, optional): Transaction ID if part of transaction
        """
        table_path = os.path.join(self.base_path, f"{table_name}.json")
        
        # Update cache
        self.cache[table_name] = data
        self.cache_hits += 1
        
        # Handle transaction
        if transaction_id:
            self.transaction_manager.add_operation(transaction_id, 
                ('write', table_name, data))
        
        with self.lock:
            with open(table_path, 'w') as f:
                json.dump(data, f, indent=2)
                
        # Update metadata
        self._update_metadata(table_name, len(data))
        
        self.logger.info(f"Wrote {len(data)} records to {table_name}")

    def read_data(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Read data from a table file with caching

        Args:
            table_name (str): Name of the table

        Returns:
            List[Dict]: Data from the table
        """
        if table_name in self.cache:
            self.cache_hits += 1
            return self.cache[table_name]
            
        table_path = os.path.join(self.base_path, f"{table_name}.json")
        if not os.path.exists(table_path):
            self.cache_misses += 1
            return []

        with self.lock:
            with open(table_path, 'r') as f:
                data = json.load(f)
                self.cache[table_name] = data
                self.cache_hits += 1
                
        self._update_metadata(table_name, len(data))
        return data

    def _update_metadata(self, table_name: str, row_count: int):
        """Update table metadata"""
        metadata_path = os.path.join(self.base_path, f"{table_name}_meta.json")
        if os.path.exists(metadata_path):
            with open(metadata_path, 'r+') as f:
                metadata = json.load(f)
                metadata['last_modified'] = datetime.now().isoformat()
                metadata['statistics']['row_count'] = row_count
                metadata['statistics']['last_access'] = datetime.now().isoformat()
                f.seek(0)
                json.dump(metadata, f, indent=2)
                f.truncate()

    def create_index(self, table_name: str, column: str):
        """
        Create an index on a column

        Args:
            table_name (str): Name of the table
            column (str): Column to index
        """
        self.index_manager.create_index(table_name, column)
        self.logger.info(f"Created index on {table_name}.{column}")

    def drop_index(self, table_name: str, column: str):
        """
        Drop an index on a column

        Args:
            table_name (str): Name of the table
            column (str): Column to drop index from
        """
        self.index_manager.drop_index(table_name, column)
        self.logger.info(f"Dropped index on {table_name}.{column}")

    def optimize_storage(self):
        """Optimize storage by cleaning up and defragmenting"""
        self._cleanup_cache()
        self._defragment_files()
        self._run_gc()

    def _cleanup_cache(self):
        """Cleanup cache based on LRU"""
        if len(self.cache) > self.cache_size:
            least_recent = heapq.nsmallest(1, self.cache.items(), 
                key=lambda x: x[1]['last_access'])[0][0]
            del self.cache[least_recent]

    def _defragment_files(self):
        """Defragment storage files"""
        for file in os.listdir(self.base_path):
            if file.endswith('.json'):
                filepath = os.path.join(self.base_path, file)
                # Implement defragmentation logic
                pass

    def _run_gc(self):
        """Run garbage collection"""
        gc.collect()

class TransactionManager:
    def __init__(self):
        self.transactions = {}
        self.lock = threading.Lock()
        self.executor = ThreadPoolExecutor(max_workers=4)

    def begin_transaction(self) -> str:
        """Begin a new transaction"""
        tx_id = str(uuid.uuid4())
        self.transactions[tx_id] = {
            'operations': [],
            'status': 'active',
            'start_time': time.time()
        }
        return tx_id

    def add_operation(self, tx_id: str, operation: Tuple[str, Any, Any]):
        """
        Add an operation to a transaction

        Args:
            tx_id (str): Transaction ID
            operation (Tuple): Operation tuple (type, table, data)
        """
        with self.lock:
            if tx_id not in self.transactions:
                raise ValueError(f"Transaction {tx_id} not found")
            self.transactions[tx_id]['operations'].append(operation)

    def commit(self, tx_id: str):
        """
        Commit a transaction

        Args:
            tx_id (str): Transaction ID
        """
        with self.lock:
            if tx_id not in self.transactions:
                raise ValueError(f"Transaction {tx_id} not found")
            
            tx = self.transactions[tx_id]
            if tx['status'] != 'active':
                raise ValueError(f"Transaction {tx_id} is not active")
                
            # Execute all operations
            for op in tx['operations']:
                self.executor.submit(self._execute_operation, op)
                
            tx['status'] = 'committed'
            tx['end_time'] = time.time()

    def rollback(self, tx_id: str):
        """
        Rollback a transaction

        Args:
            tx_id (str): Transaction ID
        """
        with self.lock:
            if tx_id not in self.transactions:
                raise ValueError(f"Transaction {tx_id} not found")
            
            tx = self.transactions[tx_id]
            if tx['status'] != 'active':
                raise ValueError(f"Transaction {tx_id} is not active")
                
            # Reverse operations
            for op in reversed(tx['operations']):
                self._reverse_operation(op)
                
            tx['status'] = 'rolled_back'
            tx['end_time'] = time.time()

class BackupManager:
    def __init__(self, base_path: str):
        self.base_path = base_path
        self.backup_path = os.path.join(base_path, 'backups')
        os.makedirs(self.backup_path, exist_ok=True)
        self.backup_interval = 3600  # 1 hour
        self.last_backup = 0
        self.backup_thread = threading.Thread(target=self._run_backup)
        self.backup_thread.daemon = True
        self.backup_thread.start()

    def create_backup(self):
        """Create a full backup"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_dir = os.path.join(self.backup_path, f'backup_{timestamp}')
        os.makedirs(backup_dir)
        
        # Copy all database files
        for file in os.listdir(self.base_path):
            if file != 'backups':
                src = os.path.join(self.base_path, file)
                dst = os.path.join(backup_dir, file)
                shutil.copy2(src, dst)
                
        # Create backup metadata
        metadata = {
            'timestamp': timestamp,
            'backup_type': 'full',
            'size': sum(os.path.getsize(f) for f in os.listdir(backup_dir) if os.path.isfile(f))
        }
        with open(os.path.join(backup_dir, 'backup_metadata.json'), 'w') as f:
            json.dump(metadata, f, indent=2)

    def restore_backup(self, backup_dir: str):
        """
        Restore from a backup

        Args:
            backup_dir (str): Path to backup directory
        """
        if not os.path.exists(backup_dir):
            raise ValueError(f"Backup directory {backup_dir} not found")
            
        # Stop all database operations
        # Copy files back
        for file in os.listdir(backup_dir):
            src = os.path.join(backup_dir, file)
            dst = os.path.join(self.base_path, file)
            shutil.copy2(src, dst)

class IndexManager:
    def __init__(self, base_path: str):
        self.base_path = base_path
        self.index_path = os.path.join(base_path, 'indexes')
        os.makedirs(self.index_path, exist_ok=True)
        self.indexes = {}
        self.lock = threading.Lock()

    def create_index(self, table_name: str, column: str):
        """
        Create an index on a column

        Args:
            table_name (str): Name of the table
            column (str): Column to index
        """
        index_name = f"{table_name}_{column}_idx"
        index_file = os.path.join(self.index_path, f"{index_name}.json")
        
        with self.lock:
            if os.path.exists(index_file):
                raise ValueError(f"Index {index_name} already exists")
                
            # Create B-tree index structure
            index = {
                'type': 'btree',
                'data': {},
                'stats': {
                    'size': 0,
                    'last_update': datetime.now().isoformat()
                }
            }
            with open(index_file, 'w') as f:
                json.dump(index, f, indent=2)
                
            self.indexes[index_name] = index_file

    def drop_index(self, table_name: str, column: str):
        """
        Drop an index on a column

        Args:
            table_name (str): Name of the table
            column (str): Column to drop index from
        """
        index_name = f"{table_name}_{column}_idx"
        index_file = os.path.join(self.index_path, f"{index_name}.json")
        
        with self.lock:
            if index_name not in self.indexes:
                raise ValueError(f"Index {index_name} not found")
                
            if os.path.exists(index_file):
                os.remove(index_file)
            del self.indexes[index_name]

    def create_table_file(self, table_name: str) -> str:
        """
        Create a new table file

        Args:
            table_name (str): Name of the table

        Returns:
            str: Path to the created table file
        """
        table_path = os.path.join(self.base_path, f"{table_name}.json")
        with open(table_path, 'w') as f:
            json.dump([], f)
        return table_path

    def write_data(self, table_name: str, data: List[Dict[str, Any]]):
        """
        Write data to a table file

        Args:
            table_name (str): Name of the table
            data (List[Dict]): Data to write
        """
        table_path = os.path.join(self.base_path, f"{table_name}.json")
        with self.lock:
            with open(table_path, 'w') as f:
                json.dump(data, f, indent=2)

    def read_data(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Read data from a table file

        Args:
            table_name (str): Name of the table

        Returns:
            List[Dict]: Data from the table
        """
        table_path = os.path.join(self.base_path, f"{table_name}.json")
        if not os.path.exists(table_path):
            return []

        with self.lock:
            with open(table_path, 'r') as f:
                return json.load(f)

class QueryOptimizer:
    def __init__(self):
        self.indexes = {}
        self.stats = defaultdict(int)

    def optimize(self, parsed_query: Dict) -> Dict:
        """
        Optimize a parsed query

        Args:
            parsed_query (Dict): Parsed query dictionary

        Returns:
            Dict: Optimized query
        """
        query_type = parsed_query['type']
        
        if query_type == 'select':
            return self._optimize_select(parsed_query)
        elif query_type == 'insert':
            return self._optimize_insert(parsed_query)
        elif query_type == 'update':
            return self._optimize_update(parsed_query)
        elif query_type == 'delete':
            return self._optimize_delete(parsed_query)
        else:
            return parsed_query

    def _optimize_select(self, parsed_query: Dict) -> Dict:
        """Optimize SELECT query"""
        optimized = parsed_query.copy()
        
        # Analyze WHERE conditions
        if optimized['where']:
            optimized['where'] = self._optimize_conditions(optimized['where'])
            
        # Analyze JOIN conditions
        if optimized['join_table']:
            optimized['join_condition'] = self._optimize_join(optimized['join_condition'])
            
        # Add index hints
        optimized['index_hints'] = self._generate_index_hints(optimized)
        
        return optimized

    def _optimize_conditions(self, conditions: List[Dict]) -> List[Dict]:
        """Optimize WHERE conditions"""
        optimized = []
        
        # Group conditions by columns
        column_groups = defaultdict(list)
        for cond in conditions:
            column_groups[cond['left']].append(cond)
        
        # Optimize each group
        for col, group in column_groups.items():
            # Check if there's an index on this column
            if col in self.indexes:
                # Use index for equality conditions
                equality_conds = [c for c in group if c['operator'] == QueryParser.Operator.EQUAL]
                if equality_conds:
                    optimized.extend(equality_conds)
                    continue
                
            # Keep other conditions
            optimized.extend(group)
        
        return optimized

    def _optimize_join(self, join_condition: str) -> str:
        """Optimize JOIN condition"""
        # Check if there are indexes on join columns
        left_col, right_col = join_condition.split('=')
        left_col = left_col.strip()
        right_col = right_col.strip()
        
        if left_col in self.indexes or right_col in self.indexes:
            # Use index for join
            return f"INDEX JOIN {join_condition}"
        
        return join_condition

    def _generate_index_hints(self, parsed_query: Dict) -> List[str]:
        """Generate index hints for query"""
        hints = []
        
        # Add hints for WHERE conditions
        if parsed_query['where']:
            for cond in parsed_query['where']:
                if cond['left'] in self.indexes:
                    hints.append(f"USE INDEX ({cond['left']})")
        
        # Add hints for JOIN
        if parsed_query['join_table']:
            left_col, right_col = parsed_query['join_condition'].split('=')
            left_col = left_col.strip()
            right_col = right_col.strip()
            
            if left_col in self.indexes:
                hints.append(f"USE INDEX ({left_col})")
            if right_col in self.indexes:
                hints.append(f"USE INDEX ({right_col})")
        
        return hints

    def _optimize_insert(self, parsed_query: Dict) -> Dict:
        """Optimize INSERT query"""
        optimized = parsed_query.copy()
        
        # Check if we can use batch insert
        if len(optimized['values']) > 1:
            optimized['batch_size'] = 1000
        
        return optimized

    def _optimize_update(self, parsed_query: Dict) -> Dict:
        """Optimize UPDATE query"""
        optimized = parsed_query.copy()
        
        # Optimize WHERE conditions
        if optimized['where']:
            optimized['where'] = self._optimize_conditions(optimized['where'])
        
        return optimized

    def _optimize_delete(self, parsed_query: Dict) -> Dict:
        """Optimize DELETE query"""
        optimized = parsed_query.copy()
        
        # Optimize WHERE conditions
        if optimized['where']:
            optimized['where'] = self._optimize_conditions(optimized['where'])
        
        return optimized

class CacheManager:
    def __init__(self, max_size: int = 1000):
        """
        Initialize cache manager

        Args:
            max_size (int): Maximum number of cache entries
        """
        self.max_size = max_size
        self.cache = {}
        self.access_times = {}
        self.lock = threading.Lock()
        self.stats = {
            'hits': 0,
            'misses': 0,
            'evictions': 0
        }

    def __getitem__(self, key: str) -> Any:
        """
        Get item from cache

        Args:
            key (str): Cache key

        Returns:
            Any: Cached value
        """
        with self.lock:
            if key in self.cache:
                self.stats['hits'] += 1
                self.access_times[key] = time.time()
                return self.cache[key]
            
            self.stats['misses'] += 1
            return None

    def __setitem__(self, key: str, value: Any) -> None:
        """
        Set item in cache

        Args:
            key (str): Cache key
            value (Any): Value to cache
        """
        with self.lock:
            if len(self.cache) >= self.max_size:
                self._evict_least_recently_used()
                
            self.cache[key] = value
            self.access_times[key] = time.time()

    def __contains__(self, key: str) -> bool:
        """
        Check if key exists in cache

        Args:
            key (str): Cache key

        Returns:
            bool: True if key exists, False otherwise
        """
        with self.lock:
            return key in self.cache

    def generate_cache_key(self, query: str) -> str:
        """
        Generate a cache key for a query

        Args:
            query (str): SQL query

        Returns:
            str: Cache key
        """
        # Generate hash of query
        query_hash = hashlib.sha256(query.encode()).hexdigest()
        return query_hash

    def _evict_least_recently_used(self) -> None:
        """Evict least recently used item from cache"""
        with self.lock:
            least_recent = min(self.access_times.items(), key=lambda x: x[1])[0]
            del self.cache[least_recent]
            del self.access_times[least_recent]
            self.stats['evictions'] += 1

    def clear(self) -> None:
        """Clear the entire cache"""
        with self.lock:
            self.cache.clear()
            self.access_times.clear()
            self.stats['evictions'] += len(self.cache)

    def get_stats(self) -> Dict:
        """Get cache statistics"""
        with self.lock:
            return self.stats.copy()

class QueryParser:
    class Operator(Enum):
        EQUAL = '='
        NOT_EQUAL = '!='
        GREATER = '>'
        LESS = '<'
        GREATER_EQUAL = '>='
        LESS_EQUAL = '<='
        LIKE = 'LIKE'
        IN = 'IN'
        BETWEEN = 'BETWEEN'
        AND = 'AND'
        OR = 'OR'
        NOT = 'NOT'

    @staticmethod
    def parse_create_table(query: str) -> Dict[str, Any]:
        """
        Parse CREATE TABLE query with advanced features

        Args:
            query (str): CREATE TABLE query

        Returns:
            Dict: Parsed table information
        """
        pattern = r'CREATE\s+TABLE\s+(\w+)\s*\((.*?)\)'
        match = re.match(pattern, query, re.IGNORECASE)

        if not match:
            raise ValueError("Invalid CREATE TABLE syntax")

        table_name = match.group(1)
        columns_str = match.group(2)

        columns = []
        constraints = []
        
        # Parse each column definition
        for column_def in columns_str.split(','):            column_def = column_def.strip()
            parts = column_def.split()
            
            # Basic column definition
            name = parts[0]
            data_type = parts[1]
            
            # Parse constraints
            constraints_dict = {}
            i = 2
            while i < len(parts):
                constraint = parts[i].upper()
                if constraint == 'PRIMARY':
                    constraints_dict['primary_key'] = True
                elif constraint == 'UNIQUE':
                    constraints_dict['unique'] = True
                elif constraint == 'NOT':
                    constraints_dict['not_null'] = True
                elif constraint == 'DEFAULT':
                    constraints_dict['default'] = parts[i+1]
                    i += 1
                elif constraint == 'CHECK':
                    constraints_dict['check'] = parts[i+1:i+4]
                    i += 3
                i += 1

            columns.append({
                'name': name,
                'type': data_type,
                'constraints': constraints_dict
            })

        return {
            'type': 'create_table',
            'table_name': table_name,
            'columns': columns,
            'constraints': constraints
        }

    @staticmethod
    def parse_insert(query: str) -> Dict[str, Any]:
        """
        Parse INSERT query with advanced features

        Args:
            query (str): INSERT query

        Returns:
            Dict: Parsed insert information
        """
        pattern = r'INSERT\s+INTO\s+(\w+)\s*(?:\((.*?)\))?\s*VALUES\s*\((.*?)\)'
        match = re.match(pattern, query, re.IGNORECASE)

        if not match:
            raise ValueError("Invalid INSERT syntax")

        table_name = match.group(1)
        columns = [col.strip() for col in match.group(2).split(',') if match.group(2)]
        values = []
        
        # Parse values
        value_str = match.group(3)
        current_value = []
        in_string = False
        escape = False
        
        for char in value_str:
            if escape:
                current_value.append(char)
                escape = False
                continue
            
            if char == '\\':
                escape = True
                continue
            
            if char in ('"', "'"):
                in_string = not in_string
                continue
            
            if char == ',' and not in_string:
                values.append(''.join(current_value).strip())
                current_value = []
                continue
            
            current_value.append(char)
        
        if current_value:
            values.append(''.join(current_value).strip())

        return {
            'type': 'insert',
            'table_name': table_name,
            'columns': columns,
            'values': values
        }

    @staticmethod
    def parse_select(query: str) -> Dict[str, Any]:
        """
        Parse SELECT query with advanced features

        Args:
            query (str): SELECT query

        Returns:
            Dict: Parsed select information
        """
        # Handle complex SELECT with JOINs and WHERE clauses
        pattern = r'SELECT\s+(.*?)\s+FROM\s+(\w+)(?:\s+(?:INNER|LEFT|RIGHT)\s+JOIN\s+(\w+)\s+ON\s+(.*))?\s*(?:WHERE\s+(.*))?\s*(?:ORDER\s+BY\s+(.*))?\s*(?:LIMIT\s+(\d+))?'
        match = re.match(pattern, query, re.IGNORECASE)

        if not match:
            raise ValueError("Invalid SELECT syntax")

        columns = [col.strip() for col in match.group(1).split(',')]
        table_name = match.group(2)
        join_table = match.group(3)
        join_condition = match.group(4)
        where_clause = match.group(5)
        order_by = match.group(6)
        limit = match.group(7)

        # Parse WHERE clause
        conditions = []
        if where_clause:
            conditions = QueryParser._parse_where_clause(where_clause)

        return {
            'type': 'select',
            'columns': columns,
            'table_name': table_name,
            'join_table': join_table,
            'join_condition': join_condition,
            'where': conditions,
            'order_by': order_by,
            'limit': int(limit) if limit else None
        }

    @staticmethod
    def _parse_where_clause(clause: str) -> List[Dict]:
        """
        Parse WHERE clause into conditions

        Args:
            clause (str): WHERE clause string

        Returns:
            List[Dict]: List of parsed conditions
        """
        conditions = []
        current_condition = []
        in_string = False
        escape = False
        paren_count = 0
        
        for char in clause:
            if escape:
                current_condition.append(char)
                escape = False
                continue
            
            if char == '\\':
                escape = True
                continue
            
            if char in ('"', "'"):
                in_string = not in_string
                continue
            
            if char == '(':
                paren_count += 1
                current_condition.append(char)
                continue
            
            if char == ')':
                paren_count -= 1
                current_condition.append(char)
                continue
            
            if char == ' ' and not in_string and paren_count == 0:
                if current_condition:
                    conditions.append(''.join(current_condition).strip())
                    current_condition = []
                continue
            
            current_condition.append(char)
        
        if current_condition:
            conditions.append(''.join(current_condition).strip())

        # Parse individual conditions
        parsed_conditions = []
        for cond in conditions:
            if '(' in cond:
                # Handle sub-conditions
                parsed_conditions.extend(QueryParser._parse_where_clause(cond[1:-1]))
            else:
                # Parse simple condition
                parts = cond.split()
                if len(parts) >= 3:
                    left = parts[0]
                    operator = QueryParser._get_operator(parts[1])
                    right = ' '.join(parts[2:])
                    parsed_conditions.append({
                        'left': left,
                        'operator': operator,
                        'right': right
                    })

        return parsed_conditions

    @staticmethod
    def _get_operator(op_str: str) -> 'QueryParser.Operator':
        """
        Get operator enum from string

        Args:
            op_str (str): Operator string

        Returns:
            QueryParser.Operator: Operator enum
        """
        op_str = op_str.upper()
        if op_str == '=':
            return QueryParser.Operator.EQUAL
        elif op_str == '!=':
            return QueryParser.Operator.NOT_EQUAL
        elif op_str == '>':
            return QueryParser.Operator.GREATER
        elif op_str == '<':
            return QueryParser.Operator.LESS
        elif op_str == '>=':
            return QueryParser.Operator.GREATER_EQUAL
        elif op_str == '<=':
            return QueryParser.Operator.LESS_EQUAL
        elif op_str == 'LIKE':
            return QueryParser.Operator.LIKE
        elif op_str == 'IN':
            return QueryParser.Operator.IN
        elif op_str == 'BETWEEN':
            return QueryParser.Operator.BETWEEN
        elif op_str == 'AND':
            return QueryParser.Operator.AND
        elif op_str == 'OR':
            return QueryParser.Operator.OR
        elif op_str == 'NOT':
            return QueryParser.Operator.NOT
        else:
            raise ValueError(f"Unknown operator: {op_str}")

class DatabaseEngine:
    def __init__(self, name: str = 'default_db', max_connections: int = 10):
        """
        Initialize the database engine with connection pooling

        Args:
            name (str): Name of the database
            max_connections (int): Maximum number of concurrent connections
        """
        self.name = name
        self.storage_engine = StorageEngine()
        self.tables = {}
        self.connection_pool = queue.Queue(maxsize=max_connections)
        self._initialize_connection_pool()
        self.logger = logging.getLogger('DatabaseEngine')
        self.logger.setLevel(logging.INFO)
        self.setup_logging()
        self.query_optimizer = QueryOptimizer()
        self.cache_manager = CacheManager()
        self.stats = defaultdict(int)

    def setup_logging(self):
        """Setup logging configuration"""
        log_file = os.path.join(self.storage_engine.base_path, 'database.log')
        handler = logging.FileHandler(log_file)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

    def _initialize_connection_pool(self):
        """Initialize connection pool"""
        for _ in range(self.connection_pool.maxsize):
            self.connection_pool.put(self._create_connection())

    def _create_connection(self):
        """Create a new database connection"""
        return {
            'id': str(uuid.uuid4()),
            'created_at': datetime.now(),
            'active': True,
            'transaction': None
        }

    def get_connection(self):
        """Get a connection from the pool"""
        try:
            return self.connection_pool.get(timeout=5)
        except queue.Empty:
            raise TimeoutError("No available connections in pool")

    def release_connection(self, connection):
        """Release a connection back to the pool"""
        if connection['active']:
            self.connection_pool.put(connection)

    def execute_query(self, query: str, connection=None) -> Union[List[Dict], None]:
        """
        Execute a SQL-like query with optimization and caching

        Args:
            query (str): Query to execute
            connection: Database connection object

        Returns:
            Result of the query
        """
        self.stats['total_queries'] += 1
        
        # Get connection if not provided
        if not connection:
            connection = self.get_connection()
            try:
                result = self._execute_query_with_connection(query, connection)
                return result
            finally:
                self.release_connection(connection)
        else:
            return self._execute_query_with_connection(query, connection)

    def _execute_query_with_connection(self, query: str, connection):
        """Execute query with specific connection"""
        query = query.strip()
        
        # Check cache first
        cache_key = self.cache_manager.generate_cache_key(query)
        if cache_key in self.cache_manager:
            self.stats['cache_hits'] += 1
            return self.cache_manager[cache_key]

        try:
            if query.upper().startswith('CREATE TABLE'):
                parsed_query = QueryParser.parse_create_table(query)
                return self._create_table(parsed_query)

            elif query.upper().startswith('INSERT INTO'):
                parsed_query = QueryParser.parse_insert(query)
                return self._insert(parsed_query, connection)

            elif query.upper().startswith('SELECT'):
                parsed_query = QueryParser.parse_select(query)
                return self._select(parsed_query)

            elif query.upper().startswith('UPDATE'):
                parsed_query = QueryParser.parse_update(query)
                return self._update(parsed_query, connection)

            elif query.upper().startswith('DELETE'):
                parsed_query = QueryParser.parse_delete(query)
                return self._delete(parsed_query, connection)

            elif query.upper().startswith('CREATE INDEX'):
                parsed_query = QueryParser.parse_create_index(query)
                return self._create_index(parsed_query)

            elif query.upper().startswith('DROP INDEX'):
                parsed_query = QueryParser.parse_drop_index(query)
                return self._drop_index(parsed_query)

            elif query.upper().startswith('BEGIN TRANSACTION'):
                return self._begin_transaction(connection)

            elif query.upper().startswith('COMMIT'):
                return self._commit_transaction(connection)

            elif query.upper().startswith('ROLLBACK'):
                return self._rollback_transaction(connection)

            else:
                raise ValueError(f"Unsupported query type: {query}")

        except Exception as e:
            self.logger.error(f"Query execution error: {e}")
            raise

    def _create_table(self, parsed_query: Dict) -> None:
        """Create a new table with advanced features"""
        table_name = parsed_query['table_name']
        columns = parsed_query['columns']
        constraints = parsed_query['constraints']

        # Create table file
        table_path = self.storage_engine.create_table_file(table_name)
        
        # Create metadata
        metadata = {
            'columns': columns,
            'constraints': constraints,
            'indexes': {},
            'statistics': {
                'row_count': 0,
                'last_access': datetime.now().isoformat()
            }
        }

        # Create metadata file
        metadata_path = os.path.join(self.storage_engine.base_path, f"{table_name}_meta.json")
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)

        # Apply constraints
        for constraint in constraints:
            if constraint['type'] == 'primary_key':
                self._create_primary_key_index(table_name, constraint['columns'])
            elif constraint['type'] == 'unique':
                self._create_unique_index(table_name, constraint['columns'])

        self.tables[table_name] = metadata
        self.logger.info(f"Created table {table_name} with {len(columns)} columns")

    def _insert(self, parsed_query: Dict, connection) -> None:
        """Insert data with transaction support"""
        table_name = parsed_query['table_name']
        columns = parsed_query['columns']
        values = parsed_query['values']

        # Check constraints
        self._validate_constraints(table_name, columns, values)

        # Get existing data
        existing_data = self.storage_engine.read_data(table_name)
        
        # Create new record
        new_record = {}
        if not columns:  # INSERT INTO table VALUES (...)
            columns = [col['name'] for col in self.tables[table_name]['columns']]
        
        for col, val in zip(columns, values):
            new_record[col] = val

        # Apply default values for missing columns
        for col in self.tables[table_name]['columns']:
            if col['name'] not in new_record and 'default' in col['constraints']:
                new_record[col['name']] = col['constraints']['default']

        # Add to data
        existing_data.append(new_record)
        
        # Write with transaction
        tx_id = connection['transaction']
        self.storage_engine.write_data(table_name, existing_data, tx_id)

        # Update indexes
        self._update_indexes(table_name, new_record)

        self.stats['inserts'] += 1
        self.logger.info(f"Inserted record into {table_name}")

    def _select(self, parsed_query: Dict) -> List[Dict]:
        """Select data with optimization"""
        table_name = parsed_query['table_name']
        columns = parsed_query['columns']
        where_conditions = parsed_query['where']
        join_table = parsed_query['join_table']
        join_condition = parsed_query['join_condition']
        order_by = parsed_query['order_by']
        limit = parsed_query['limit']

        # Optimize query
        optimized_query = self.query_optimizer.optimize(parsed_query)

        # Get data
        data = self.storage_engine.read_data(table_name)

        # Apply WHERE conditions
        if where_conditions:
            data = self._filter_data(data, where_conditions)

        # Handle JOIN
        if join_table:
            data = self._perform_join(data, join_table, join_condition)

        # Select specific columns
        if columns != ['*']:
            data = [{col: record[col] for col in columns if col in record} 
                    for record in data]

        # Apply ORDER BY
        if order_by:
            data = self._sort_data(data, order_by)

        # Apply LIMIT
        if limit:
            data = data[:limit]

        # Cache result
        cache_key = self.cache_manager.generate_cache_key(parsed_query)
        self.cache_manager[cache_key] = data

        self.stats['selects'] += 1
        return data

    def _update(self, parsed_query: Dict, connection) -> None:
        """Update data with transaction support"""
        table_name = parsed_query['table_name']
        set_clause = parsed_query['set']
        where_conditions = parsed_query['where']

        # Get existing data
        data = self.storage_engine.read_data(table_name)

        # Find matching records
        matching_records = self._filter_data(data, where_conditions)

        # Update matching records
        for record in matching_records:
            for col, val in set_clause.items():
                record[col] = val

        # Write with transaction
        tx_id = connection['transaction']
        self.storage_engine.write_data(table_name, data, tx_id)

        # Update indexes
        for record in matching_records:
            self._update_indexes(table_name, record)

        self.stats['updates'] += 1
        self.logger.info(f"Updated {len(matching_records)} records in {table_name}")

    def _delete(self, parsed_query: Dict, connection) -> None:
        """Delete data with transaction support"""
        table_name = parsed_query['table_name']
        where_conditions = parsed_query['where']

        # Get existing data
        data = self.storage_engine.read_data(table_name)

        # Find matching records
        matching_records = self._filter_data(data, where_conditions)

        # Remove matching records
        data = [record for record in data if record not in matching_records]

        # Write with transaction
        tx_id = connection['transaction']
        self.storage_engine.write_data(table_name, data, tx_id)

        # Update indexes
        for record in matching_records:
            self._update_indexes(table_name, record, delete=True)

        self.stats['deletes'] += 1
        self.logger.info(f"Deleted {len(matching_records)} records from {table_name}")

    def _begin_transaction(self, connection) -> None:
        """Begin a new transaction"""
        tx_id = self.storage_engine.transaction_manager.begin_transaction()
        connection['transaction'] = tx_id
        self.stats['transactions'] += 1
        self.logger.info(f"Transaction {tx_id} started")

    def _commit_transaction(self, connection) -> None:
        """Commit current transaction"""
        tx_id = connection['transaction']
        if tx_id:
            self.storage_engine.transaction_manager.commit(tx_id)
            connection['transaction'] = None
            self.stats['commits'] += 1
            self.logger.info(f"Transaction {tx_id} committed")

    def _rollback_transaction(self, connection) -> None:
        """Rollback current transaction"""
        tx_id = connection['transaction']
        if tx_id:
            self.storage_engine.transaction_manager.rollback(tx_id)
            connection['transaction'] = None
            self.stats['rollbacks'] += 1
            self.logger.info(f"Transaction {tx_id} rolled back")

    def _create_index(self, parsed_query: Dict) -> None:
        """Create an index on a table"""
        table_name = parsed_query['table_name']
        column = parsed_query['column']
        index_type = parsed_query.get('type', 'btree')

        self.storage_engine.create_index(table_name, column, index_type)
        self.stats['indexes_created'] += 1
        self.logger.info(f"Created index on {table_name}.{column}")

    def _drop_index(self, parsed_query: Dict) -> None:
        """Drop an index from a table"""
        table_name = parsed_query['table_name']
        column = parsed_query['column']

        self.storage_engine.drop_index(table_name, column)
        self.stats['indexes_dropped'] += 1
        self.logger.info(f"Dropped index on {table_name}.{column}")

    def _validate_constraints(self, table_name: str, columns: List[str], values: List[Any]) -> None:
        """Validate data against table constraints"""
        metadata = self.tables[table_name]
        
        for col, val in zip(columns, values):
            for column_meta in metadata['columns']:
                if column_meta['name'] == col:
                    # Check NOT NULL constraint
                    if 'not_null' in column_meta['constraints'] and val is None:
                        raise ValueError(f"Column {col} cannot be NULL")
                    
                    # Check CHECK constraint
                    if 'check' in column_meta['constraints']:
                        check_expr = column_meta['constraints']['check']
                        if not eval(check_expr, {}, {col: val}):
                            raise ValueError(f"Value {val} fails CHECK constraint for {col}")

    def _filter_data(self, data: List[Dict], conditions: List[Dict]) -> List[Dict]:
        """Filter data based on conditions"""
        filtered = data[:]
        
        for condition in conditions:
            operator = condition['operator']
            column = condition['left']
            value = condition['right']
            
            if operator == QueryParser.Operator.EQUAL:
                filtered = [r for r in filtered if r.get(column) == value]
            elif operator == QueryParser.Operator.NOT_EQUAL:
                filtered = [r for r in filtered if r.get(column) != value]
            elif operator == QueryParser.Operator.GREATER:
                filtered = [r for r in filtered if r.get(column) > value]
            elif operator == QueryParser.Operator.LESS:
                filtered = [r for r in filtered if r.get(column) < value]
            elif operator == QueryParser.Operator.GREATER_EQUAL:
                filtered = [r for r in filtered if r.get(column) >= value]
            elif operator == QueryParser.Operator.LESS_EQUAL:
                filtered = [r for r in filtered if r.get(column) <= value]
            elif operator == QueryParser.Operator.LIKE:
                pattern = re.compile(value.replace('%', '.*'))
                filtered = [r for r in filtered if pattern.match(str(r.get(column)))]
            elif operator == QueryParser.Operator.IN:
                filtered = [r for r in filtered if r.get(column) in value]
            elif operator == QueryParser.Operator.BETWEEN:
                low, high = value.split(' AND ')
                filtered = [r for r in filtered if low <= r.get(column) <= high]

        return filtered

    def _sort_data(self, data: List[Dict], order_by: str) -> List[Dict]:
        """Sort data based on ORDER BY clause"""
        columns = [col.strip() for col in order_by.split(',')]
        sort_keys = []
        
        for col in columns:
            desc = False
            if col.endswith(' DESC'):
                desc = True
                col = col[:-5].strip()
            elif col.endswith(' ASC'):
                col = col[:-4].strip()
            
            sort_keys.append((col, desc))
        
        def sort_key_func(record):
            return tuple(
                record.get(col) if not desc else -record.get(col)
                for col, desc in sort_keys
            )
        
        return sorted(data, key=sort_key_func)

    def _perform_join(self, data: List[Dict], join_table: str, join_condition: str) -> List[Dict]:
        """Perform JOIN operation"""
        # Get join table data
        join_data = self.storage_engine.read_data(join_table)
        
        # Parse join condition
        left_col, right_col = join_condition.split('=')
        left_col = left_col.strip()
        right_col = right_col.strip()
        
        # Perform join
        result = []
        for record in data:
            for join_record in join_data:
                if record.get(left_col) == join_record.get(right_col):
                    # Combine records
                    combined = record.copy()
                    combined.update({f"{join_table}_{k}": v 
                                   for k, v in join_record.items()})
                    result.append(combined)
        
        return result

    def _update_indexes(self, table_name: str, record: Dict, delete: bool = False) -> None:
        """Update indexes for a record"""
        metadata = self.tables[table_name]
        
        for index_name, index_info in metadata['indexes'].items():
            if delete:
                self.storage_engine.index_manager.delete_from_index(
                    table_name, index_name, record)
            else:
                self.storage_engine.index_manager.add_to_index(
                    table_name, index_name, record)

    def execute_query(self, query: str) -> Union[List[Dict], None]:
        """
        Execute a SQL-like query

        Args:
            query (str): Query to execute

        Returns:
            Result of the query
        """
        query = query.strip()

        try:
            if query.upper().startswith('CREATE TABLE'):
                parsed_query = QueryParser.parse_create_table(query)
                return self._create_table(parsed_query)

            elif query.upper().startswith('INSERT INTO'):
                parsed_query = QueryParser.parse_insert(query)
                return self._insert(parsed_query)

            elif query.upper().startswith('SELECT'):
                parsed_query = QueryParser.parse_select(query)
                return self._select(parsed_query)

            else:
                raise ValueError(f"Unsupported query type: {query}")

        except Exception as e:
            print(f"Query execution error: {e}")
            return None

    def _create_table(self, parsed_query: Dict[str, Any]):
        """
        Create a new table

        Args:
            parsed_query (Dict): Parsed CREATE TABLE query
        """
        table_name = parsed_query['table_name']
        self.tables[table_name] = parsed_query['columns']
        self.storage_engine.create_table_file(table_name)
        return f"Table {table_name} created successfully"

    def _insert(self, parsed_query: Dict[str, Any]):
        """
        Insert data into a table

        Args:
            parsed_query (Dict): Parsed INSERT query
        """
        table_name = parsed_query['table_name']
        columns = parsed_query['columns']
        values = parsed_query['values']

        
        record = dict(zip(columns, values))
        record['id'] = str(uuid.uuid4())  
        record['created_at'] = datetime.now().isoformat()

       
        existing_data = self.storage_engine.read_data(table_name)
        existing_data.append(record)

       
        self.storage_engine.write_data(table_name, existing_data)
        return "Record inserted successfully"

    def _select(self, parsed_query: Dict[str, Any]):
      """
      Select data from a table

      Args:
          parsed_query (Dict): Parsed SELECT query

      Returns:
          List of matching records
      """
      table_name = parsed_query['table_name']
      columns = parsed_query['columns']
      where_clause = parsed_query.get('where')
      data = self.storage_engine.read_data(table_name)

      print("Debug - Full Data:", data)  
      print("Debug - Where Clause:", where_clause)  

      if where_clause:
      
          filtered_data = []
          for record in data:
              try:
                  
                  field, value = [part.strip() for part in where_clause.split('=')]
                  value = value.strip("'\"") 

                  print(f"Debug - Checking: {field} == {value}")  
                  print(f"Debug - Record: {record}")

                 
                  if str(record.get(field)) == str(value):
                      filtered_data.append(record)
              except Exception as e:
                  print(f"Error filtering record: {e}")

          
          if columns[0] != '*':
              filtered_data = [{col: record.get(col) for col in columns} for record in filtered_data]

          print("Debug - Filtered Data:", filtered_data)  
          return filtered_data
      else:
          
          if columns[0] != '*':
              return [{col: record.get(col) for col in columns} for record in data]
          return data

