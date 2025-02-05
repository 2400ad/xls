import tkinter as tk
from tkinter import ttk, scrolledtext
from maptest import ColumnMapper

class MapperGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("DB Column Mapper")
        self.mapper = ColumnMapper()
        
        # DB 정보 프레임
        self.create_db_frame()
        
        # 컬럼 정보 프레임
        self.create_column_frame()
        
        # 결과 프레임
        self.create_result_frame()
        
        # 실행 버튼
        self.create_execute_button()
        
    def create_db_frame(self):
        db_frame = ttk.LabelFrame(self.root, text="DB 정보", padding="5")
        db_frame.pack(fill="x", padx=5, pady=5)
        
        # 송신 DB 정보
        ttk.Label(db_frame, text="송신 DB:").grid(row=0, column=0, sticky="e")
        self.send_db = ttk.Entry(db_frame)
        self.send_db.grid(row=0, column=1, padx=5, pady=2)
        
        ttk.Label(db_frame, text="송신 테이블:").grid(row=1, column=0, sticky="e")
        self.send_table = ttk.Entry(db_frame)
        self.send_table.grid(row=1, column=1, padx=5, pady=2)
        
        # 수신 DB 정보
        ttk.Label(db_frame, text="수신 DB:").grid(row=0, column=2, sticky="e")
        self.recv_db = ttk.Entry(db_frame)
        self.recv_db.grid(row=0, column=3, padx=5, pady=2)
        
        ttk.Label(db_frame, text="수신 테이블:").grid(row=1, column=2, sticky="e")
        self.recv_table = ttk.Entry(db_frame)
        self.recv_table.grid(row=1, column=3, padx=5, pady=2)
        
    def create_column_frame(self):
        column_frame = ttk.LabelFrame(self.root, text="컬럼 정보", padding="5")
        column_frame.pack(fill="both", expand=True, padx=5, pady=5)
        
        # 송신 컬럼
        ttk.Label(column_frame, text="송신 컬럼:").grid(row=0, column=0, sticky="nw")
        self.send_columns = scrolledtext.ScrolledText(column_frame, width=40, height=10)
        self.send_columns.grid(row=1, column=0, padx=5, pady=2)
        
        # 수신 컬럼
        ttk.Label(column_frame, text="수신 컬럼:").grid(row=0, column=1, sticky="nw")
        self.recv_columns = scrolledtext.ScrolledText(column_frame, width=40, height=10)
        self.recv_columns.grid(row=1, column=1, padx=5, pady=2)
        
    def create_result_frame(self):
        result_frame = ttk.LabelFrame(self.root, text="비교 결과", padding="5")
        result_frame.pack(fill="both", expand=True, padx=5, pady=5)
        
        self.result_text = scrolledtext.ScrolledText(result_frame, width=80, height=10)
        self.result_text.pack(fill="both", expand=True)
        
    def create_execute_button(self):
        ttk.Button(self.root, text="비교 실행", command=self.execute_comparison).pack(pady=5)
        
    def execute_comparison(self):
        # DB 정보 설정
        self.mapper.set_db_info(
            self.send_db.get(),
            self.send_table.get(),
            self.recv_db.get(),
            self.recv_table.get()
        )
        
        # 컬럼 정보 설정
        self.mapper.set_columns(
            self.send_columns.get("1.0", tk.END),
            self.recv_columns.get("1.0", tk.END)
        )
        
        try:
            # 비교 실행
            results = self.mapper.compare_columns()
            
            # 결과 표시
            self.result_text.delete("1.0", tk.END)
            for result in results:
                line = (f"송신컬럼: {result['send_column']}, "
                       f"수신컬럼: {result['recv_column']}\n"
                       f"Type 다름: {result['type_diff']}\n"
                       f"Size 다름: {result['size_diff']}\n"
                       f"1024byte 이상: {result['over_1024']}\n"
                       f"Nullable 다름: {result['nullable_diff']}\n"
                       f"{'='*50}\n")
                self.result_text.insert(tk.END, line)
                
        except Exception as e:
            self.result_text.delete("1.0", tk.END)
            self.result_text.insert(tk.END, f"오류 발생: {str(e)}")

if __name__ == "__main__":
    root = tk.Tk()
    app = MapperGUI(root)
    root.mainloop()