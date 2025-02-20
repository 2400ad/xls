import openpyxl
from xltest import read_interface_block, process_interface
from comp_q import QueryParser, QueryDifference, FileSearcher
from maptest import ColumnMapper
import xml.etree.ElementTree as ET
from typing import Dict, List, Optional, Tuple
import os

class XMLComparator:
    def __init__(self, excel_path: str, search_dir: str):
        """
        XML 비교를 위한 클래스 초기화
        
        Args:
            excel_path (str): 인터페이스 정보가 있는 Excel 파일 경로
            search_dir (str): XML 파일을 검색할 디렉토리 경로
        """
        self.excel_path = excel_path
        self.search_dir = search_dir
        self.workbook = openpyxl.load_workbook(excel_path)
        self.worksheet = self.workbook.active
        self.mapper = ColumnMapper()
        self.query_parser = QueryParser()
        self.file_searcher = FileSearcher()
        
    def extract_from_xml(self, xml_path: str) -> Tuple[str, str]:
        """
        XML 파일에서 쿼리와 XML 내용을 추출합니다.
        
        Args:
            xml_path (str): XML 파일 경로
            
        Returns:
            Tuple[str, str]: (쿼리, XML 내용)
        """
        if not xml_path or not os.path.exists(xml_path):
            return None, None
            
        try:
            # XML 파일 전체 내용 읽기
            with open(xml_path, 'r', encoding='utf-8') as f:
                xml_content = f.read()
            
            # QueryParser를 사용하여 쿼리 추출
            select_queries, insert_queries = self.query_parser.parse_xml_file(xml_path)
            
            # 파일이 .SND.xml로 끝나면 SELECT 쿼리를, .RCV.xml로 끝나면 INSERT 쿼리를 사용
            query = None
            if xml_path.endswith('.SND.xml') and select_queries:
                query = select_queries[0]  # 첫 번째 SELECT 쿼리 사용
            elif xml_path.endswith('.RCV.xml') and insert_queries:
                query = insert_queries[0]  # 첫 번째 INSERT 쿼리 사용
            
            return query, xml_content
            
        except Exception as e:
            print(f"XML 파일 처리 중 오류 발생: {str(e)}")
            return None, None
            
    def compare_queries(self, query1: str, query2: str) -> QueryDifference:
        """
        두 쿼리를 비교합니다.
        
        Args:
            query1 (str): 첫 번째 쿼리
            query2 (str): 두 번째 쿼리
            
        Returns:
            QueryDifference: 쿼리 비교 결과
        """
        if not query1 or not query2:
            return None
        return self.query_parser.compare_queries(query1, query2)
        
    def find_interface_files(self, if_id: str) -> Dict[str, str]:
        """
        주어진 IF ID에 해당하는 송수신 XML 파일을 찾습니다.
        
        Args:
            if_id (str): 인터페이스 ID
            
        Returns:
            Dict[str, str]: {'send': 송신파일경로, 'recv': 수신파일경로}
        """
        results = self.file_searcher.find_files_with_keywords(
            self.search_dir, 
            [f"{if_id}*.SND.xml", f"{if_id}*.RCV.xml"]
        )
        
        files = {'send': None, 'recv': None}
        for file_path in results.get(if_id, []):
            if file_path.endswith('.SND.xml'):
                files['send'] = file_path
            elif file_path.endswith('.RCV.xml'):
                files['recv'] = file_path
        return files
        
    def process_interface_block(self, start_col: int) -> Optional[Dict]:
        """
        Excel에서 하나의 인터페이스 블록을 처리합니다.
        
        Args:
            start_col (int): 인터페이스 블록이 시작되는 컬럼
            
        Returns:
            Optional[Dict]: 처리된 인터페이스 정보와 결과, 실패시 None
        """
        # xltest.py의 함수를 사용하여 Excel에서 정보 읽기
        interface_info = read_interface_block(self.worksheet, start_col)
        if not interface_info:
            return None
            
        # Excel에서 추출된 쿼리와 XML 얻기
        excel_results = process_interface(interface_info, self.mapper)
        
        # IF ID를 사용하여 관련 파일 찾기
        if_id = interface_info['interface_id']
        interface_files = self.find_interface_files(if_id)
        
        # XML 파일에서 쿼리와 XML 추출
        file_results = {
            'send': {'query': None, 'xml': None},
            'recv': {'query': None, 'xml': None}
        }
        
        if interface_files['send']:
            file_results['send']['query'], file_results['send']['xml'] = \
                self.extract_from_xml(interface_files['send'])
                
        if interface_files['recv']:
            file_results['recv']['query'], file_results['recv']['xml'] = \
                self.extract_from_xml(interface_files['recv'])
        
        # 쿼리 비교
        comparisons = {
            'send': None,
            'recv': None
        }
        
        if excel_results['send_sql'] and file_results['send']['query']:
            comparisons['send'] = self.compare_queries(
                excel_results['send_sql'],
                file_results['send']['query']
            )
            
        if excel_results['recv_sql'] and file_results['recv']['query']:
            comparisons['recv'] = self.compare_queries(
                excel_results['recv_sql'],
                file_results['recv']['query']
            )
        
        return {
            'interface_info': interface_info,
            'excel_results': excel_results,
            'file_results': file_results,
            'files': interface_files,
            'comparisons': comparisons
        }
        
    def process_all_interfaces(self) -> List[Dict]:
        """
        Excel 파일의 모든 인터페이스를 처리합니다.
        B열부터 시작하여 3컬럼 단위로 처리합니다.
        
        Returns:
            List[Dict]: 각 인터페이스의 처리 결과 목록
        """
        results = []
        current_col = 2  # B열부터 시작
        
        while current_col <= self.worksheet.max_column:
            result = self.process_interface_block(current_col)
            if not result:
                break
                
            results.append(result)
            current_col += 3  # 다음 인터페이스 블록으로 이동 (3컬럼 단위)
            
        return results
        
    def close(self):
        """리소스 정리"""
        self.workbook.close()
        if self.mapper:
            self.mapper.close_connections()

def main():
    """
    메인 실행 함수
    """
    excel_path = "input.xlsx"
    search_dir = "."  # 현재 디렉토리에서 검색
    
    comparator = XMLComparator(excel_path, search_dir)
    try:
        results = comparator.process_all_interfaces()
        
        # 결과 출력
        for idx, result in enumerate(results, 1):
            interface_info = result['interface_info']
            excel_results = result['excel_results']
            file_results = result['file_results']
            files = result['files']
            comparisons = result['comparisons']
            
            print(f"\n=== 인터페이스 {idx} ===")
            print(f"ID: {interface_info['interface_id']}")
            print(f"이름: {interface_info['interface_name']}")
            
            print("\n파일 검색 결과:")
            print(f"송신 파일: {files['send']}")
            print(f"수신 파일: {files['recv']}")
            
            print("\n쿼리 비교 결과:")
            if comparisons['send']:
                print("송신 쿼리:")
                if comparisons['send'].is_equal:
                    print("  일치")
                else:
                    print("  불일치:")
                    for diff in comparisons['send'].differences:
                        print(f"    컬럼: {diff['column']}")
                        print(f"    Excel: {diff['query1_value']}")
                        print(f"    파일: {diff['query2_value']}")
                        
            if comparisons['recv']:
                print("수신 쿼리:")
                if comparisons['recv'].is_equal:
                    print("  일치")
                else:
                    print("  불일치:")
                    for diff in comparisons['recv'].differences:
                        print(f"    컬럼: {diff['column']}")
                        print(f"    Excel: {diff['query1_value']}")
                        print(f"    파일: {diff['query2_value']}")
            
            if excel_results['errors']:
                print("\n오류:")
                for error in excel_results['errors']:
                    print(f"  - {error}")
                    
    finally:
        comparator.close()

if __name__ == "__main__":
    main()
