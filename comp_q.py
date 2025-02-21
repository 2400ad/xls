import xml.etree.ElementTree as ET
import re
from typing import Dict, List, Tuple, Optional

class QueryDifference:
    def __init__(self):
        self.is_equal = True
        self.differences = []
        self.query_type = None
        self.table_name = None
    
    def add_difference(self, column: str, value1: str, value2: str):
        self.is_equal = False
        self.differences.append({
            'column': column,
            'query1_value': value1,
            'query2_value': value2
        })

class QueryParser:
    def __init__(self):
        self.select_queries = []
        self.insert_queries = []
        # 특수 컬럼 정의
        self.special_columns = {
            'send': {
                'required': ['EAI_SEQ_ID', 'DATA_INTERFACE_TYPE_CODE'],
                'mappings': []  # 추가 매핑을 저장할 리스트
            },
            'recv': {
                'required': [
                    'EAI_SEQ_ID',
                    'DATA_INTERFACE_TYPE_CODE',
                    'EAI_INTERFACE_DATE',
                    'APPLICATION_TRANSFER_FLAG'
                ],
                'special_values': {
                    'EAI_INTERFACE_DATE': 'SYSDATE',
                    'APPLICATION_TRANSFER_FLAG': "'N'"
                }
            }
        }
    
    @staticmethod
    def normalize_query(query):
        """
        Normalize a SQL query by removing extra whitespace and standardizing format
        
        Args:
            query (str): SQL query to normalize
            
        Returns:
            str: Normalized query
        """
        # Convert to lowercase for case-insensitive comparison
        query = query.lower()
        
        # Remove comments if any
        query = re.sub(r'--.*$', '', query, flags=re.MULTILINE)
        
        # Replace multiple whitespace with single space
        query = re.sub(r'\s+', ' ', query)
        
        # Remove whitespace around common SQL punctuation
        query = re.sub(r'\s*(,|\(|\))\s*', r'\1', query)
        
        # Ensure single space after SQL keywords
        query = re.sub(r'(select|from|where|into|values)\s+', r'\1 ', query)
        
        return query.strip()

    @staticmethod
    def parse_select_columns(query) -> Optional[Dict[str, str]]:
        """Extract columns from SELECT query and return as dictionary"""
        query = QueryParser.normalize_query(query)
        match = re.match(r'select\s+(.+?)\s+from', query)
        if not match:
            return None
        
        columns = {}
        for col in match.group(1).split(','):
            col = col.strip()
            # Use the full column expression as both key and value
            columns[col] = col
        return columns

    @staticmethod
    def parse_insert_parts(query) -> Optional[Tuple[str, Dict[str, str]]]:
        """Extract and return table name and column-value pairs from INSERT query"""
        query = QueryParser.normalize_query(query)
        
        # Extract table name
        table_match = re.search(r'insert\s+into\s+(\w+\.?\w+)\s*\(', query)
        if not table_match:
            return None
        table_name = table_match.group(1)
        
        # Extract columns and values
        cols_match = re.search(r'\((.*?)\)\s*values\s*\((.*?)\)', query)
        if not cols_match:
            return None
            
        columns = [col.strip() for col in cols_match.group(1).split(',')]
        values = [val.strip() for val in cols_match.group(2).split(',')]
        
        # Create column-value dictionary
        col_val_dict = dict(zip(columns, values))
        
        return table_name, col_val_dict

    @staticmethod
    def compare_queries(query1: str, query2: str) -> QueryDifference:
        """
        Compare two SQL queries and return detailed differences
        
        Args:
            query1 (str): First SQL query
            query2 (str): Second SQL query
            
        Returns:
            QueryDifference: Object containing comparison results and differences
        """
        result = QueryDifference()
        
        # 쿼리 정규화
        norm_query1 = QueryParser.normalize_query(query1)
        norm_query2 = QueryParser.normalize_query(query2)
        
        # 쿼리 타입 확인
        if norm_query1.startswith('select'):
            result.query_type = 'SELECT'
            columns1 = QueryParser.parse_select_columns(query1)
            columns2 = QueryParser.parse_select_columns(query2)
            table1 = QueryParser.extract_table_name(query1)
            table2 = QueryParser.extract_table_name(query2)
        elif norm_query1.startswith('insert'):
            result.query_type = 'INSERT'
            columns1 = QueryParser.parse_insert_parts(query1)[1]
            columns2 = QueryParser.parse_insert_parts(query2)[1]
            table1 = QueryParser.extract_table_name(query1)
            table2 = QueryParser.extract_table_name(query2)
        else:
            raise ValueError("지원하지 않는 쿼리 타입입니다.")
            
        result.table_name = table1
        
        # 특수 컬럼 제외
        direction = 'recv' if result.query_type == 'INSERT' else 'send'
        special_cols = set(QueryParser.special_columns[direction]['required'])
        
        # 일반 컬럼만 비교
        columns1 = {k: v for k, v in columns1.items() if k.upper() not in special_cols}
        columns2 = {k: v for k, v in columns2.items() if k.upper() not in special_cols}
        
        # 컬럼 비교
        all_columns = set(columns1.keys()) | set(columns2.keys())
        for col in all_columns:
            if col not in columns1:
                result.add_difference(col, None, columns2[col])
            elif col not in columns2:
                result.add_difference(col, columns1[col], None)
            elif columns1[col] != columns2[col]:
                result.add_difference(col, columns1[col], columns2[col])
                
        return result

    def check_special_columns(self, query: str, direction: str) -> List[str]:
        """
        특수 컬럼의 존재 여부와 값을 체크합니다.
        
        Args:
            query (str): 검사할 쿼리
            direction (str): 송신('send') 또는 수신('recv')
            
        Returns:
            List[str]: 경고 메시지 리스트
        """
        warnings = []
        norm_query = self.normalize_query(query)
        
        if direction == 'send':
            columns = self.parse_select_columns(query)
        else:
            columns = self.parse_insert_parts(query)[1]
        
        # 대문자로 변환하여 비교
        columns = {k.upper(): v for k, v in columns.items()}
        
        # 필수 특수 컬럼 체크
        for col in self.special_columns[direction]['required']:
            if col not in columns:
                warnings.append(f"필수 특수 컬럼 '{col}'이(가) {direction} 쿼리에 없습니다.")
        
        # 수신 쿼리의 특수 값 체크
        if direction == 'recv':
            for col, expected_value in self.special_columns[direction]['special_values'].items():
                if col in columns and columns[col].upper() != expected_value.upper():
                    warnings.append(f"특수 컬럼 '{col}'의 값이 '{expected_value}'이(가) 아닙니다. (현재 값: {columns[col]})")
        
        return warnings

    def clean_select_query(self, query):
        """
        Clean SELECT query by removing WHERE clause
        """
        # Find the position of WHERE (case insensitive)
        where_match = re.search(r'\sWHERE\s', query, re.IGNORECASE)
        if where_match:
            # Return only the part before WHERE
            return query[:where_match.start()].strip()
        return query.strip()

    def clean_insert_query(self, query):
        """
        Clean INSERT query by removing PL/SQL blocks
        """
        # Extract only the INSERT ... VALUES part
        insert_match = re.search(
            r'INSERT\s+INTO\s+[^;]+VALUES\s*\([^)]+\)',
            query,
            re.IGNORECASE | re.MULTILINE | re.DOTALL
        )
        if insert_match:
            return insert_match.group(0).strip()
        return query.strip()

    @staticmethod
    def is_meaningful_query(query: str) -> bool:
        """
        Check if a query is meaningful (not just a simple existence check or count)
        
        Args:
            query (str): SQL query to analyze
            
        Returns:
            bool: True if the query is meaningful, False otherwise
        """
        query = query.lower()
        
        # Remove comments and normalize whitespace
        query = re.sub(r'--.*$', '', query, flags=re.MULTILINE)
        query = ' '.join(query.split())
        
        # Patterns for meaningless queries
        meaningless_patterns = [
            r'select\s+1\s+from',  # SELECT 1 FROM ...
            r'select\s+count\s*\(\s*\*\s*\)\s+from',  # SELECT COUNT(*) FROM ...
            r'select\s+count\s*\(\s*1\s*\)\s+from',  # SELECT COUNT(1) FROM ...
            r'select\s+null\s+from',  # SELECT NULL FROM ...
            r'select\s+\'[^\']*\'\s+from',  # SELECT 'constant' FROM ...
            r'select\s+\d+\s+from',  # SELECT {number} FROM ...
        ]
        
        # Check if query matches any meaningless pattern
        for pattern in meaningless_patterns:
            if re.search(pattern, query):
                return False
                
        # For SELECT queries, check if it's selecting actual columns
        if query.startswith('select'):
            # Extract the SELECT clause (between SELECT and FROM)
            select_match = re.match(r'select\s+(.+?)\s+from', query)
            if select_match:
                select_clause = select_match.group(1)
                # If only selecting literals or simple expressions, consider it meaningless
                if re.match(r'^[\d\'\"\s,]+$', select_clause):
                    return False
        
        return True

    @classmethod
    def find_files_by_table(cls, folder_path: str, table_name: str, skip_meaningless: bool = True) -> dict:
        """
        Find files containing queries that reference the specified table
        
        Args:
            folder_path (str): Path to the folder to search in
            table_name (str): Name of the DB table to search for
            skip_meaningless (bool): If True, skip queries that appear to be meaningless
            
        Returns:
            dict: Dictionary with 'select' and 'insert' as keys, each containing a list of tuples
                 where each tuple contains (file_path, query)
        """
        import os
        
        results = {
            'select': [],
            'insert': []
        }
        
        # Normalize table name for comparison
        table_name = table_name.lower()
        
        # Create parser instance for processing files
        parser = cls()
        
        # Walk through all files in the folder
        for root, _, files in os.walk(folder_path):
            for file in files:
                file_path = os.path.join(root, file)
                
                # Skip non-XML files silently
                if not file_path.lower().endswith('.xml'):
                    continue
                    
                try:
                    # Try to parse queries from the file
                    select_queries, insert_queries = parser.parse_xml_file(file_path)
                    
                    # Check SELECT queries
                    for query in select_queries:
                        if cls.extract_table_name(query).lower() == table_name:
                            # Skip meaningless queries if requested
                            if skip_meaningless and not cls.is_meaningful_query(query):
                                continue
                            rel_path = os.path.relpath(file_path, folder_path)
                            results['select'].append((rel_path, query))
                    
                    # Check INSERT queries
                    for query in insert_queries:
                        if cls.extract_table_name(query).lower() == table_name:
                            rel_path = os.path.relpath(file_path, folder_path)
                            results['insert'].append((rel_path, query))
                    
                except Exception:
                    # Skip any errors silently
                    continue
        
        return results

    def parse_xml_file(self, filename):
        """
        Parse XML file and extract SQL queries
        
        Args:
            filename (str): Path to the XML file
            
        Returns:
            tuple: Lists of (select_queries, insert_queries)
        """
        try:
            # Clear previous queries
            self.select_queries = []
            self.insert_queries = []
            
            # Parse XML file
            tree = ET.parse(filename)
            root = tree.getroot()
            
            # Find all text content in the XML
            for elem in root.iter():
                if elem.text:
                    text = elem.text.strip()
                    # Extract SELECT queries
                    if re.search(r'SELECT\s+', text, re.IGNORECASE):
                        cleaned_query = self.clean_select_query(text)
                        self.select_queries.append(cleaned_query)
                    # Extract INSERT queries
                    elif re.search(r'INSERT\s+INTO\s+', text, re.IGNORECASE):
                        cleaned_query = self.clean_insert_query(text)
                        self.insert_queries.append(cleaned_query)
            
            return self.select_queries, self.insert_queries
            
        except ET.ParseError:
            return [], []
        except Exception:
            return [], []
    
    def get_select_queries(self):
        """Return list of extracted SELECT queries"""
        return self.select_queries
    
    def get_insert_queries(self):
        """Return list of extracted INSERT queries"""
        return self.insert_queries
    
    def print_queries(self):
        """Print all extracted queries"""
        print("\nSELECT Queries:")
        for i, query in enumerate(self.select_queries, 1):
            print(f"{i}. {query}\n")
            
        print("\nINSERT Queries:")
        for i, query in enumerate(self.insert_queries, 1):
            print(f"{i}. {query}\n")

    @staticmethod
    def print_query_differences(diff: QueryDifference):
        """Print the differences between two queries in a readable format"""
        print(f"\nQuery Type: {diff.query_type}")
        if diff.is_equal:
            print("Queries are equivalent")
        else:
            print("Differences found:")
            for d in diff.differences:
                print(f"- Column '{d['column']}':")
                print(f"  Query 1: {d['query1_value']}")
                print(f"  Query 2: {d['query2_value']}")

    @staticmethod
    def extract_table_name(query: str) -> str:
        """
        Extract table name from a SQL query
        
        Args:
            query (str): SQL query to analyze
            
        Returns:
            str: Table name or empty string if not found
        """
        query = QueryParser.normalize_query(query)
        
        # For SELECT queries
        select_match = re.search(r'from\s+([a-zA-Z0-9_$.]+)', query)
        if select_match:
            return select_match.group(1)
            
        # For INSERT queries
        insert_match = re.search(r'insert\s+into\s+([a-zA-Z0-9_$.]+)', query)
        if insert_match:
            return insert_match.group(1)
            
        return ""

    @classmethod
    def print_table_search_results(cls, results: dict, table_name: str):
        """
        Print table search results in a formatted way
        
        Args:
            results (dict): Dictionary with search results
            table_name (str): Name of the DB table that was searched
        """
        print(f"\nFiles and queries referencing table: {table_name}")
        print("=" * 50)
        
        print("\nSELECT queries found in:")
        if results['select']:
            for i, (file, query) in enumerate(results['select'], 1):
                print(f"\n{i}. File: {file}")
                print("Query:")
                print(query)
        else:
            print("  No files found with SELECT queries")
            
        print("\nINSERT queries found in:")
        if results['insert']:
            for i, (file, query) in enumerate(results['insert'], 1):
                print(f"\n{i}. File: {file}")
                print("Query:")
                print(query)
        else:
            print("  No files found with INSERT queries")
        
        print("\n" + "=" * 50)

class FileSearcher:
    @staticmethod
    def find_files_with_keywords(folder_path: str, keywords: list) -> dict:
        """
        Search for files in the given folder that contain any of the specified keywords
        
        Args:
            folder_path (str): Path to the folder to search in
            keywords (list): List of keywords to search for
            
        Returns:
            dict: Dictionary with keyword as key and list of matching files as value
        """
        import os
        
        # Initialize results dictionary
        results = {keyword: [] for keyword in keywords}
        
        # Walk through all files in the folder
        for root, _, files in os.walk(folder_path):
            for file in files:
                file_path = os.path.join(root, file)
                
                try:
                    # Try to read file content
                    with open(file_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                        
                    # Check for each keyword
                    for keyword in keywords:
                        if keyword in content:
                            # Store relative path instead of full path
                            rel_path = os.path.relpath(file_path, folder_path)
                            results[keyword].append(rel_path)
                            
                except (UnicodeDecodeError, IOError):
                    # Skip files that can't be read as text
                    continue
        
        return results

    @staticmethod
    def print_search_results(results: dict):
        """
        Print search results in a formatted way
        
        Args:
            results (dict): Dictionary with keyword as key and list of matching files as value
        """
        print("\nSearch Results:")
        print("=" * 50)
        
        for keyword, files in results.items():
            print(f"\nKeyword: {keyword}")
            if files:
                print("Found in files:")
                for i, file in enumerate(files, 1):
                    print(f"  {i}. {file}")
            else:
                print("No files found containing this keyword")
        
        print("\n" + "=" * 50)

# Test the query comparison
if __name__ == "__main__":
    # Test SELECT queries with different column orders
    select1 = """
    select a, b, c from d
    """
    select2 = """
    select b, a, c 
    from d
    """
    
    # Test INSERT queries with different values
    insert1 = """
    INSERT INTO table1 (a, b, c) values ( :a, 'N', :code)
    """
    insert2 = """
    INSERT INTO table1 (a, c, b) 
    values 
    ( :a, :code, 'Y')
    """
    
    # Compare queries and print differences
    select_diff = QueryParser.compare_queries(select1, select2)
    insert_diff = QueryParser.compare_queries(insert1, insert2)
    
    print("\nComparing SELECT queries:")
    print(f"Query 1:\n{select1}")
    print(f"Query 2:\n{select2}")
    QueryParser.print_query_differences(select_diff)
    
    print("\nComparing INSERT queries:")
    print(f"Query 1:\n{insert1}")
    print(f"Query 2:\n{insert2}")
    QueryParser.print_query_differences(insert_diff)

    # Test with the example queries from the previous test
    qry1 = """
    INSERT INTO A2EDC_MGR.TB_EDC_IFI_ECO_TARGET_GLS_N_I (
         EAI_SEQ_ID, DATA_INTERFACE_TYPE_CODE,
         EAI_INTERFACE_DATE, APPLICATION_TRANSFER_FLAG,
         ECO_ID, VALIDATION_SEQS,
         EMEMO_ID, LOT_ID,
         GLASS_ID, SLOT_ID,
         CASSETTE_ID, FMC_REQUEST_TITLE
            )
    VALUES (
         :EAI_SEQ_ID, :DATA_INTERFACE_TYPE_CODE,
         SYSDATE, 'N',
         :ECO_ID, :VALIDATION_SEQS,
         :EMEMO_ID, :LOT_ID,
         :GLASS_ID, :SLOT_ID,
         :CASSETTE_ID, :FMC_REQUEST_TITLE
    )
    """
    qry2 = """
    INSERT INTO A2EDC_MGR.TB_EDC_IFI_ECO_TARGET_GLS_N_I (
         EAI_SEQ_ID, 
         DATA_INTERFACE_TYPE_CODE,

         EAI_INTERFACE_DATE, APPLICATION_TRANSFER_FLAG,
         ECO_ID, VALIDATION_SEQS,
         EMEMO_ID, LOT_ID,
         GLASS_ID, SLOT_ID,
         CASSETTE_ID, FMC_REQUEST_TITLE
            )
    VALUES (
         :EAI_SEQ_ID, 
         :DATA_INTERFACE_TYPE_CODE,
         SYSDATE, 'Y',
         :ECO_ID, 
         :VALIDATION_SEQS,
         :EMEMO_ID, :LOT_ID,
         :GLASS_ID, :SLOT_ID,
         :CASSETTE_ID, :FMC_REQUEST_TITLE
    )
    """
    
    complex_diff = QueryParser.compare_queries(qry1, qry2)
    print("\nComparing complex INSERT queries:")
    QueryParser.print_query_differences(complex_diff)

    # Test with XML file if provided
    try:
        parser = QueryParser()
        select_queries, insert_queries = parser.parse_xml_file("mq_snd.xml")
        parser.print_queries()
    except:
        pass

    parser = QueryParser()
    select_queries, insert_queries = parser.parse_xml_file("mq_rcv.xml")
    parser.print_queries()  # To see all extracted queries

    # Test file search functionality
    print("\nTesting file search functionality:")
    folder_path = "."  # Current directory
    keywords = ["GSMOD.H8ASSY", "RCV"]
    
    searcher = FileSearcher()
    results = searcher.find_files_with_keywords(folder_path, keywords)
    searcher.print_search_results(results)

    # Test table search functionality
    print("\nTesting table search functionality:")
    folder_path = "."  # Current directory
    table_name = "XXWMSV.XXWMSV_KIT_RCPT_S_I"  # Example table name
    
    parser = QueryParser()
    table_results = parser.find_files_by_table(folder_path, table_name)
    parser.print_table_search_results(table_results, table_name)
