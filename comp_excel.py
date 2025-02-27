import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter
import os
from typing import Dict, List, Optional, Tuple
import datetime


def read_interface_block(ws, start_col):
    """Excel에서 3컬럼 단위로 하나의 인터페이스 정보를 읽습니다.
    """
    try:
        interface_info = {
            'interface_name': ws.cell(row=1, column=start_col).value or '',  # IF NAME (1행)
            'interface_id': ws.cell(row=2, column=start_col).value or '',    # IF ID (2행)
            'send': {'owner': None, 'table_name': None, 'columns': [], 'db_info': None},
            'recv': {'owner': None, 'table_name': None, 'columns': [], 'db_info': None}
        }
        
        # 인터페이스 ID가 없으면 빈 인터페이스로 간주
        if not interface_info['interface_id']:
            return None
            
        # DB 연결 정보 (3행에서 읽기)
        send_db_info = eval(ws.cell(row=3, column=start_col).value or '{}')
        recv_db_info = eval(ws.cell(row=3, column=start_col + 1).value or '{}')
        interface_info['send']['db_info'] = send_db_info
        interface_info['recv']['db_info'] = recv_db_info
        
        # 테이블 정보 (4행에서 읽기)
        send_table_info = eval(ws.cell(row=4, column=start_col).value or '{}')
        recv_table_info = eval(ws.cell(row=4, column=start_col + 1).value or '{}')
        
        interface_info['send']['owner'] = send_table_info.get('owner')
        interface_info['send']['table_name'] = send_table_info.get('table_name')
        interface_info['recv']['owner'] = recv_table_info.get('owner')
        interface_info['recv']['table_name'] = recv_table_info.get('table_name')
        
        # 컬럼 매핑 정보 (5행부터)
        row = 5
        while True:
            send_col = ws.cell(row=row, column=start_col).value
            recv_col = ws.cell(row=row, column=start_col + 1).value
            
            # 둘 다 None이면 컬럼 매핑 끝
            if send_col is None and recv_col is None:
                break
                
            # 송신 컬럼 추가
            if send_col:
                interface_info['send']['columns'].append(send_col)
                
            # 수신 컬럼 추가
            if recv_col:
                interface_info['recv']['columns'].append(recv_col)
                
            row += 1
        
        return interface_info
        
    except Exception as e:
        print(f"인터페이스 정보 읽기 오류: {e}")
        return None


class ExcelManager:
    """
    Excel 파일 관리 및 출력을 위한 클래스
    """

    def __init__(self, excel_path: str = None):
        """
        Excel 관리자 클래스 초기화
        
        Args:
            excel_path (str, optional): 기존 엑셀 파일 경로 (없으면 새로 생성)
        """
        self.excel_path = excel_path
        if excel_path and os.path.exists(excel_path):
            self.workbook = openpyxl.load_workbook(excel_path)
        else:
            self.workbook = openpyxl.Workbook()
        
        self.output_path = ''
    
    def initialize_excel_output(self, summary_sheet_name='요약'):
        """
        결과를 저장할 새 엑셀 파일 초기화
        
        Args:
            summary_sheet_name (str): 요약 시트 이름
        """
        # 기본 시트의 이름을 변경
        if 'Sheet' in self.workbook.sheetnames:
            sheet = self.workbook['Sheet']
            sheet.title = summary_sheet_name
        elif summary_sheet_name not in self.workbook.sheetnames:
            sheet = self.workbook.create_sheet(summary_sheet_name)
            
        # 요약 시트 헤더 초기화
        self._initialize_summary_sheet(self.workbook[summary_sheet_name])
        
        return self.workbook[summary_sheet_name]
    
    def _initialize_summary_sheet(self, sheet):
        """
        요약 시트의 헤더를 초기화합니다.
        
        Args:
            sheet: 요약 시트 객체
        """
        # 스타일 정의
        header_fill = PatternFill(start_color="CCCCFF", end_color="CCCCFF", fill_type="solid")
        border = Border(
            left=Side(style='thin'),
            right=Side(style='thin'),
            top=Side(style='thin'),
            bottom=Side(style='thin')
        )
        align_center = Alignment(horizontal='center', vertical='center', wrap_text=True)
        
        # 열 너비 설정
        sheet.column_dimensions['A'].width = 15  # 인터페이스 ID
        sheet.column_dimensions['B'].width = 30  # 인터페이스 명
        sheet.column_dimensions['C'].width = 20  # 송신 테이블
        sheet.column_dimensions['D'].width = 30  # MQ 송신 파일
        sheet.column_dimensions['E'].width = 30  # BW 송신 파일
        sheet.column_dimensions['F'].width = 15  # 송신 비교 결과
        sheet.column_dimensions['G'].width = 30  # MQ 수신 파일
        sheet.column_dimensions['H'].width = 30  # BW 수신 파일
        sheet.column_dimensions['I'].width = 15  # 수신 비교 결과
        
        # 헤더 행 생성
        headers = ["인터페이스 ID", "인터페이스 명", "송신 테이블", "MQ 송신 파일", "BW 송신 파일", "송신 비교 결과", 
                  "MQ 수신 파일", "BW 수신 파일", "수신 비교 결과"]
        
        for col_idx, header in enumerate(headers, 1):
            cell = sheet.cell(row=1, column=col_idx, value=header)
            cell.font = Font(bold=True)
            cell.fill = header_fill
            cell.alignment = align_center
            cell.border = border
    
    def save_excel_output(self, output_path):
        """
        처리된 결과를 엑셀 파일로 저장
        
        Args:
            output_path (str): 출력 엑셀 파일 경로
            
        Returns:
            bool: 저장 성공 여부
        """
        # output_path 값을 인스턴스 변수에 저장
        self.output_path = output_path
        
        try:
            self.workbook.save(output_path)
            print(f"\n[결과 저장 완료] 파일 경로: {output_path}")
            return True
        except Exception as e:
            print(f"엑셀 파일 저장 중 오류 발생: {e}")
            return False
    
    def create_interface_sheet(self, if_info, result_data, sheet_index=None):
        """
        엑셀 파일에 각 인터페이스별 시트를 생성하고, 데이터를 기록
        
        Args:
            if_info (dict): 인터페이스 정보
            result_data (dict): 결과 데이터
            sheet_index (int, optional): 시트 인덱스 (없으면 자동 생성)
        """
        # 시트 이름 생성 (인터페이스 이름 또는 ID)
        sheet_name = if_info.get('interface_name', '') or if_info.get('interface_id', '')
        
        # 시트 이름이 30자를 초과하면 자르기 (Excel 시트 이름 제한)
        if len(sheet_name) > 30:
            sheet_name = sheet_name[:27] + '...'
            
        # 시트 이름이 중복되는 경우 처리
        base_name = sheet_name
        counter = 1
        while sheet_name in self.workbook.sheetnames:
            sheet_name = f"{base_name[:25]}_{counter}"
            counter += 1
        
        # 인터페이스 시트 생성
        sheet = self.workbook.create_sheet(title=sheet_name)
        
        # 스타일 정의
        header_fill = PatternFill(start_color="CCCCFF", end_color="CCCCFF", fill_type="solid")
        border = Border(
            left=Side(style='thin'),
            right=Side(style='thin'),
            top=Side(style='thin'),
            bottom=Side(style='thin')
        )
        align_center = Alignment(horizontal='center', vertical='center', wrap_text=True)
        align_left = Alignment(horizontal='left', vertical='center', wrap_text=True)
        
        # 열 너비 설정
        for col in range(1, 10):
            sheet.column_dimensions[get_column_letter(col)].width = 15
        
        # 인터페이스 정보 헤더 설정
        sheet.cell(row=1, column=1, value="인터페이스 정보").font = Font(bold=True)
        sheet.cell(row=1, column=1).fill = header_fill
        
        # 인터페이스 정보 내용 채우기
        sheet.cell(row=2, column=1, value="인터페이스 ID").font = Font(bold=True)
        sheet.cell(row=2, column=2, value=if_info.get('interface_id', ''))
        
        sheet.cell(row=3, column=1, value="인터페이스 명").font = Font(bold=True)
        sheet.cell(row=3, column=2, value=if_info.get('interface_name', ''))
        
        # 결과 데이터 채우기 - 이 부분은 호출자에 따라 맞춰서 구현 가능
        row = 5
        for key, value in result_data.items():
            if isinstance(value, dict):
                sheet.cell(row=row, column=1, value=key).font = Font(bold=True)
                row += 1
                for sub_key, sub_value in value.items():
                    sheet.cell(row=row, column=1, value=sub_key)
                    if isinstance(sub_value, str) and len(sub_value) > 100:
                        # 긴 문자열은 여러 줄로 나누어 표시
                        sheet.cell(row=row, column=2, value=sub_value)
                        sheet.cell(row=row, column=2).alignment = Alignment(wrap_text=True)
                    else:
                        sheet.cell(row=row, column=2, value=str(sub_value))
                    row += 1
            else:
                sheet.cell(row=row, column=1, value=key).font = Font(bold=True)
                sheet.cell(row=row, column=2, value=str(value))
                row += 1
                
        # 모든 셀에 테두리 적용
        for row_cells in sheet.iter_rows(min_row=1, max_row=row-1, min_col=1, max_col=2):
            for cell in row_cells:
                cell.border = border
        
        return sheet
    
    def update_summary_sheet(self, result, row, sheet_name='요약'):
        """
        요약 시트에 결과를 추가합니다.
        
        Args:
            result (dict): 인터페이스 처리 결과
            row (int): 요약 시트의 행 번호
            sheet_name (str): 요약 시트 이름
        """
        # 요약 시트 가져오기
        if sheet_name not in self.workbook.sheetnames:
            self.initialize_excel_output(sheet_name)
        
        summary_sheet = self.workbook[sheet_name]
        
        # 스타일 정의
        border = Border(
            left=Side(style='thin'),
            right=Side(style='thin'),
            top=Side(style='thin'),
            bottom=Side(style='thin')
        )
        align_center = Alignment(horizontal='center', vertical='center', wrap_text=True)
        align_left = Alignment(horizontal='left', vertical='center', wrap_text=True)
        
        # 각 열에 데이터 채우기 (호출자에 맞게 조정 가능)
        for col, value in enumerate(result, 1):
            cell = summary_sheet.cell(row=row, column=col, value=value)
            cell.border = border
            
            # 가운데 정렬 또는 왼쪽 정렬 설정
            if col in [1, 6, 9]:  # ID와 비교 결과 열은 가운데 정렬
                cell.alignment = align_center
            else:
                cell.alignment = align_left
            
            # 비교 결과에 따른 색상 설정
            if col in [6, 9] and value == "일치":
                cell.fill = PatternFill(start_color="C6EFCE", end_color="C6EFCE", fill_type="solid")
            elif col in [6, 9] and value == "차이":
                cell.fill = PatternFill(start_color="FFC7CE", end_color="FFC7CE", fill_type="solid")
            elif col in [6, 9] and value == "비교불가":
                cell.fill = PatternFill(start_color="DDDDDD", end_color="DDDDDD", fill_type="solid")
    
    def close(self):
        """
        리소스 정리
        """
        if self.workbook:
            if self.output_path:
                try:
                    self.workbook.save(self.output_path)
                except:
                    pass


# 간단한 사용 예시
def main():
    # 예시 코드
    excel_manager = ExcelManager()
    sheet = excel_manager.initialize_excel_output()
    
    # 샘플 데이터로 요약 시트 업데이트
    sample_data = ["IF001", "테스트 인터페이스", "TEST_TABLE", "test.SND.xml", "test.xml", "일치", "test.RCV.xml", "test.xml", "차이"]
    excel_manager.update_summary_sheet(sample_data, 2)
    
    # 샘플 인터페이스 시트 생성
    if_info = {
        'interface_id': 'IF001',
        'interface_name': '테스트 인터페이스'
    }
    
    result_data = {
        '송신 파일': {
            '경로': 'test.SND.xml',
            '쿼리': 'SELECT * FROM TEST_TABLE'
        },
        '수신 파일': {
            '경로': 'test.RCV.xml', 
            '쿼리': 'INSERT INTO TEST_TABLE'
        }
    }
    
    excel_manager.create_interface_sheet(if_info, result_data)
    
    # 파일 저장
    excel_manager.save_excel_output('test_output.xlsx')
    excel_manager.close()


if __name__ == "__main__":
    main()