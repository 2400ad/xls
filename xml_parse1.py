import tkinter as tk
from tkinter import messagebox
import xml.etree.ElementTree as ET
import re
from tkinterdnd2 import DND_FILES, TkinterDnD

class XMLParserApp:
    def __init__(self, root):
        self.root = root
        self.root.title("TIBCO XML SQL Parser")
        self.root.geometry("800x600")
        
        # Create main frame
        self.main_frame = tk.Frame(root)
        self.main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Create drag-drop label
        self.drop_label = tk.Label(
            self.main_frame,
            text="Drag and Drop XML File Here",
            relief="solid",
            width=40,
            height=4
        )
        self.drop_label.pack(pady=20)
        
        # Create text widget for displaying results
        self.result_text = tk.Text(self.main_frame, height=20, width=80)
        self.result_text.pack(pady=20)
        
        # Configure drag-drop
        self.drop_label.drop_target_register(DND_FILES)
        self.drop_label.dnd_bind('<<Drop>>', self.process_dropped_file)

    def process_dropped_file(self, event):
        file_path = event.data
        if not file_path.lower().endswith('.xml'):
            messagebox.showerror("Error", "Please drop an XML file")
            return
            
        try:
            self.parse_xml(file_path)
        except Exception as e:
            messagebox.showerror("Error", f"Error processing file: {str(e)}")
    
    def parse_xml(self, file_path):
        self.result_text.delete(1.0, tk.END)
        try:
            # Parse XML with namespace handling
            tree = ET.parse(file_path)
            root = tree.getroot()
            
            # Handle namespaces
            namespaces = {'pd': re.findall(r'{(.*?)}', root.tag)[0] if '}' in root.tag else ''}
            
            # Find all SQL statements
            sql_count = 0
            for activity in root.findall('.//pd:activity', namespaces):
                if activity is not None:
                    config = activity.find('.//config')
                    if config is not None:
                        statement = config.find('.//statement')
                        if statement is not None and statement.text:
                            sql_count += 1
                            activity_name = activity.get('name', 'Unknown')
                            self.result_text.insert(tk.END, f"\n--- SQL Query #{sql_count} (Activity: {activity_name}) ---\n")
                            self.result_text.insert(tk.END, statement.text.strip() + "\n")
            
            if sql_count == 0:
                self.result_text.insert(tk.END, "No SQL queries found in the XML file.")
            else:
                self.result_text.insert(tk.END, f"\nTotal SQL queries found: {sql_count}")
                
        except ET.ParseError as e:
            self.result_text.insert(tk.END, f"Error parsing XML: {str(e)}")
        except Exception as e:
            self.result_text.insert(tk.END, f"Error: {str(e)}")

def main():
    root = TkinterDnD.Tk()
    app = XMLParserApp(root)
    root.mainloop()

if __name__ == "__main__":
    main()
