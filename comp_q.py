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
    # 특수 컬럼 정의를 클래스 변수로 변경
    special_columns = {
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

    def __init__(self):
        self.select_queries = []
        self.insert_queries = []

    def normalize_query(self, query):
        """
        Normalize a SQL query by removing extra whitespace and standardizing format
        
        Args:
            query (str): SQL query to normalize
            
        Returns:
            str: Normalized query
        """
        # Remove comments if any
        query = re.sub(r'--.*$', '', query, flags=re.MULTILINE)
        
        # Replace multiple whitespace with single space
        query = re.sub(r'\s+', ' ', query)
        
        # Remove whitespace around common SQL punctuation
        query = re.sub(r'\s*(,|\(|\))\s*', r'\1', query)
        
        # Ensure single space after SQL keywords (case-insensitive match but preserve original case)
        for keyword in ['SELECT', 'FROM', 'WHERE', 'INTO', 'VALUES']:
            query = re.sub(f'(?i){keyword}\\s+', f'{keyword} ', query)
        
        return query.strip()

    def parse_select_columns(self, query) -> Optional[Dict[str, str]]:
        """Extract columns from SELECT query and return as dictionary"""
        # 정규화된 쿼리 사용
        query = self.normalize_query(query)
        
        # SELECT와 FROM 사이의 컬럼 추출
        match = re.search(r'SELECT\s+(.*?)\s+FROM', query, flags=re.IGNORECASE)
        if not match:
            return None
            
        columns = {}
        for col in match.group(1).split(','):
            col = col.strip()
            if col:  # 빈 문자열이 아닌 경우만 처리
                columns[col] = col
        return columns if columns else None

    def parse_insert_parts(self, query) -> Optional[Tuple[str, Dict[str, str]]]:
        """Extract and return table name and column-value pairs from INSERT query"""
        # 정규화된 쿼리 사용
        query = self.normalize_query(query)
        
        # INSERT INTO와 테이블 이름 추출
        table_match = re.search(r'INSERT\s+INTO\s+([A-Za-z0-9_$.]+)', query, flags=re.IGNORECASE)
        if not table_match:
            return None
        table_name = table_match.group(1)
        
        # 컬럼과 값 추출
        pattern = r'\((.*?)\)\s*VALUES\s*\((.*?)\)'
        cols_match = re.search(pattern, query, flags=re.IGNORECASE | re.DOTALL)
        if not cols_match:
            return None
            
        # 컬럼과 값 파싱
        columns = {}
        col_names = [c.strip() for c in cols_match.group(1).split(',')]
        col_values = [v.strip() for v in cols_match.group(2).split(',')]
        
        # 컬럼과 값의 개수가 일치하는지 확인
        if len(col_names) != len(col_values):
            return None
            
        # 빈 컬럼이나 값이 있는지 확인
        if not all(col_names) or not all(col_values):
            return None
            
        for name, value in zip(col_names, col_values):
            columns[name] = value
            
        return (table_name, columns) if columns else None

    def compare_queries(self, query1: str, query2: str) -> QueryDifference:
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
        norm_query1 = self.normalize_query(query1)
        norm_query2 = self.normalize_query(query2)
        
        # 쿼리 타입 확인
        if re.search(r'SELECT', norm_query1, flags=re.IGNORECASE):
            result.query_type = 'SELECT'
            columns1 = self.parse_select_columns(query1)
            columns2 = self.parse_select_columns(query2)
            table1 = self.extract_table_name(query1)
            table2 = self.extract_table_name(query2)
            
            if columns1 is None or columns2 is None:
                raise ValueError("SELECT 쿼리 파싱 실패")
                
        elif re.search(r'INSERT', norm_query1, flags=re.IGNORECASE):
            result.query_type = 'INSERT'
            insert_result1 = self.parse_insert_parts(query1)
            insert_result2 = self.parse_insert_parts(query2)
            
            if insert_result1 is None or insert_result2 is None:
                raise ValueError("INSERT 쿼리 파싱 실패")
                
            table1, columns1 = insert_result1
            table2, columns2 = insert_result2
        else:
            raise ValueError("지원하지 않는 쿼리 타입입니다.")
            
        result.table_name = table1
        
        # 특수 컬럼 제외
        direction = 'recv' if result.query_type == 'INSERT' else 'send'
        special_cols = set(self.special_columns[direction]['required'])
        
        # 일반 컬럼만 비교 (대소문자 구분 없이 비교하되 원본 케이스 유지)
        columns1_filtered = {k: v for k, v in columns1.items() if k.upper() not in special_cols}
        columns2_filtered = {k: v for k, v in columns2.items() if k.upper() not in special_cols}
        
        # 컬럼 비교
        all_columns = set(columns1_filtered.keys()) | set(columns2_filtered.keys())
        for col in all_columns:
            if col not in columns1_filtered:
                result.add_difference(col, None, columns2_filtered[col])
            elif col not in columns2_filtered:
                result.add_difference(col, columns1_filtered[col], None)
            elif columns1_filtered[col] != columns2_filtered[col]:
                result.add_difference(col, columns1_filtered[col], columns2_filtered[col])
                
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
        
        if direction == 'send':
            columns = self.parse_select_columns(query)
        else:
            _, columns = self.parse_insert_parts(query)
            
        if not columns:
            return warnings
            
        # 대소문자 구분 없이 컬럼 비교를 위한 매핑 make
        columns_upper = {k.upper(): (k, v) for k, v in columns.items()}
        
        # 필수 특수 컬럼 체크
        for col in self.special_columns[direction]['required']:
            if col not in columns_upper:
                warnings.append(f"필수 특수 컬럼 '{col}'이(가) {direction} 쿼리에 없습니다.")
        
        # 수신 쿼리의 특수 값 체크
        if direction == 'recv':
            for col, expected_value in self.special_columns[direction]['special_values'].items():
                if col in columns_upper:
                    actual_value = columns_upper[col][1].upper()
                    if actual_value != expected_value.upper():
                        warnings.append(f"특수 컬럼 '{col}'의 값이 '{expected_value}'이(가) 아닙니다. (현재 값: {columns_upper[col][1]})")
        
        return warnings

    def clean_select_query(self, query):
        """
        Clean SELECT query by removing WHERE clause
        """
        # Find the position of WHERE (case insensitive)
        where_match = re.search(r'\sWHERE\s', query, flags=re.IGNORECASE)
        if where_match:
            # Return only the part before WHERE
            return query[:where_match.start()].strip()
        return query.strip()

    def clean_insert_query(self, query: str) -> str:
        """
        Clean INSERT query by removing PL/SQL blocks
        """
        # PL/SQL 블록에서 INSERT 문 추출
        pattern = r"""
            (?:BEGIN\s+)?          # BEGIN (optional)
            (INSERT\s+INTO\s+      # INSERT INTO
            [^;]+                  # everything until semicolon
            )                      # capture this part
            (?:\s*;)?             # optional semicolon
            (?:\s*EXCEPTION\s+     # EXCEPTION block (optional)
            .*?                    # everything until END
            END;?)?                # END with optional semicolon
        """
        insert_match = re.search(
            pattern,
            query,
            flags=re.IGNORECASE | re.MULTILINE | re.DOTALL | re.VERBOSE
        )
        
        if insert_match:
            return insert_match.group(1).strip()
        return query.strip()

    def is_meaningful_query(self, query: str) -> bool:
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

    def find_files_by_table(self, folder_path: str, table_name: str, skip_meaningless: bool = True) -> dict:
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
        parser = self
        
        # Walk through all files in the folder
        for root, _, files in os.walk(folder_path):
            for file in files:
                file_path = os.path.join(root, file)
                
                # Skip non-XML files silently
                if not file_path.lower().endswith('.xml'):
                    continue
                    
                try:
                    # Try to parse queries from the file
                    select_queries, insert_queries = self.parse_xml_file(file_path)
                    
                    # Check SELECT queries
                    for query in select_queries:
                        if self.extract_table_name(query).lower() == table_name:
                            # Skip meaningless queries if requested
                            if skip_meaningless and not self.is_meaningful_query(query):
                                continue
                            rel_path = os.path.relpath(file_path, folder_path)
                            results['select'].append((rel_path, query))
                    
                    # Check INSERT queries
                    for query in insert_queries:
                        if self.extract_table_name(query).lower() == table_name:
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
                    if re.search(r'SELECT\s+', text, flags=re.IGNORECASE):
                        cleaned_query = self.clean_select_query(text)
                        self.select_queries.append(cleaned_query)
                    # Extract INSERT queries
                    elif re.search(r'INSERT\s+INTO\s+', text, flags=re.IGNORECASE):
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

    def print_query_differences(self, diff: QueryDifference):
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

    def extract_table_name(self, query: str) -> str:
        """
        Extract table name from a SQL query
        
        Args:
            query (str): SQL query to analyze
            
        Returns:
            str: Table name or empty string if not found
        """
        query = self.normalize_query(query)
        
        # For SELECT queries
        select_match = re.search(r'from\s+([a-zA-Z0-9_$.]+)', query, flags=re.IGNORECASE)
        if select_match:
            return select_match.group(1)
            
        # For INSERT queries
        insert_match = re.search(r'insert\s+into\s+([a-zA-Z0-9_$.]+)', query, flags=re.IGNORECASE)
        if insert_match:
            return insert_match.group(1)
            
        return ""

    def print_table_search_results(self, results: dict, table_name: str):
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
    # Create QueryParser instance
    parser = QueryParser()
    
    # Test SELECT queries with different column orders
    select1 = """
    SELECT a, b, c FROM d
    """
    select2 = """
    SELECT b, a, c 
    FROM d
    """
    
    # Test INSERT queries with different values
    insert1 = """
    INSERT INTO table1 (a, b, c) VALUES ( :a, 'N', :code)
    """
    insert2 = """
    INSERT INTO table1 (a, c, b) 
    VALUES 
    ( :a, :code, 'Y')
    """
    
    # Compare queries and print differences
    select_diff = parser.compare_queries(select1, select2)
    insert_diff = parser.compare_queries(insert1, insert2)
    
    print("\nComparing SELECT queries:")
    print(f"Query 1:\n{select1}")
    print(f"Query 2:\n{select2}")
    parser.print_query_differences(select_diff)
    
    print("\nComparing INSERT queries:")
    print(f"Query 1:\n{insert1}")
    print(f"Query 2:\n{insert2}")
    parser.print_query_differences(insert_diff)

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
    
    complex_diff = parser.compare_queries(qry1, qry2)
    print("\nComparing complex INSERT queries:")
    print(f"Query 1:\n{qry1}")
    print(f"Query 2:\n{qry2}")
    parser.print_query_differences(complex_diff)

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
