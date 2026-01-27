import os
import queue
import threading
import tkinter as tk
from contextlib import redirect_stdout
from pathlib import Path
from tkinter import filedialog

import customtkinter as ctk


class LogWriter:
    def __init__(self, log_queue):
        self.log_queue = log_queue

    def write(self, message):
        if message:
            self.log_queue.put(("log", message))

    def flush(self):
        pass


class UrqmdVisApp(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("urqmdVis")
        screen_w = self.winfo_screenwidth()
        screen_h = self.winfo_screenheight()
        self.geometry(f"{screen_w // 2}x{screen_h}")
        self.minsize(760, 620)
        self.log_queue = queue.Queue()
        self.project_dir = Path(__file__).resolve().parent

        ctk.set_appearance_mode("light")
        ctk.set_default_color_theme("blue")

        try:
            self.iconphoto(True, tk.PhotoImage(file=self.project_dir / "assets" / "urqmdVis-logo.png"))
        except Exception:
            pass

        self._build_ui()
        self.after(50, self._drain_queue)
        self.after(150, self._bring_to_front)

    def _bring_to_front(self):
        try:
            self.deiconify()
            self.lift()
            self.attributes("-topmost", True)
            self.after(200, lambda: self.attributes("-topmost", False))
            self.focus_force()
        except Exception:
            pass

    def _build_ui(self):
        self.configure(fg_color="#f5f6f8")

        header = ctk.CTkFrame(self, fg_color="transparent")
        header.pack(fill="x", padx=28, pady=(22, 10))
        title = ctk.CTkLabel(header, text="urqmdVis", font=ctk.CTkFont(size=26, weight="bold"))
        subtitle = ctk.CTkLabel(
            header,
            text="Local runner for UrQMD event visualization.",
            font=ctk.CTkFont(size=12),
            text_color="#1d1d1f",
        )
        title.pack(anchor="w")
        subtitle.pack(anchor="w")

        card = ctk.CTkFrame(self, fg_color="#ffffff", corner_radius=18)
        card.pack(fill="both", expand=True, padx=24, pady=(0, 24))

        form = ctk.CTkFrame(card, fg_color="transparent")
        form.pack(fill="x", padx=20, pady=18)

        self.input_file_var = tk.StringVar(value="../files/urqmd_AuAu_0-3fm/urqmd_1_14.dat")
        self.event_number_var = tk.StringVar(value="9")
        self.event_name_var = tk.StringVar(value="event")
        self.parquet_folder_var = tk.StringVar(value="outputs/parquet")
        self.csv_folder_var = tk.StringVar(value="outputs/csv")
        self.reduce_var = tk.BooleanVar(value=True)
        self.parse_var = tk.BooleanVar(value=True)
        self.convert_var = tk.BooleanVar(value=True)
        self.paraview_path_var = tk.StringVar(value="/Applications/ParaView-5.13.0.app/Contents/MacOS/paraview")

        ctk.CTkLabel(form, text="Input .dat file", text_color="#1d1d1f").grid(row=0, column=0, sticky="w")
        form.grid_columnconfigure(0, weight=1)
        form.grid_columnconfigure(1, weight=0)
        form.grid_columnconfigure(2, weight=0)
        self.input_entry = ctk.CTkEntry(form, textvariable=self.input_file_var)
        self.input_entry.grid(row=1, column=0, columnspan=2, sticky="we", pady=(6, 14))
        ctk.CTkButton(
            form,
            text="Browse",
            width=90,
            fg_color="#1d1d1f",
            hover_color="#3a3a3c",
            command=self._pick_file,
        ).grid(row=1, column=2, padx=(10, 0), pady=(6, 14), sticky="e")

        ctk.CTkLabel(form, text="Event number", text_color="#1d1d1f").grid(row=2, column=0, sticky="w")
        self.event_spin = ctk.CTkEntry(form, textvariable=self.event_number_var, width=120)
        self.event_spin.grid(row=3, column=0, sticky="w", pady=(6, 14))

        ctk.CTkLabel(form, text="Event name prefix", text_color="#1d1d1f").grid(row=2, column=1, sticky="w")
        ctk.CTkEntry(form, textvariable=self.event_name_var, width=180).grid(row=3, column=1, sticky="w", pady=(6, 14))

        ctk.CTkLabel(form, text="Parquet output folder", text_color="#1d1d1f").grid(row=4, column=0, sticky="w")
        ctk.CTkEntry(form, textvariable=self.parquet_folder_var, width=220).grid(row=5, column=0, sticky="w", pady=(6, 14))

        ctk.CTkLabel(form, text="CSV output folder", text_color="#1d1d1f").grid(row=4, column=1, sticky="w")
        ctk.CTkEntry(form, textvariable=self.csv_folder_var, width=220).grid(row=5, column=1, sticky="w", pady=(6, 14))

        ctk.CTkLabel(form, text="Steps", text_color="#1d1d1f").grid(row=6, column=0, sticky="w")
        step_row = ctk.CTkFrame(form, fg_color="transparent")
        step_row.grid(row=7, column=0, columnspan=3, sticky="w", pady=(6, 14))
        ctk.CTkCheckBox(step_row, text="Reduce", variable=self.reduce_var).pack(side="left", padx=(0, 16))
        ctk.CTkCheckBox(step_row, text="Parse to Parquet", variable=self.parse_var).pack(side="left", padx=(0, 16))
        ctk.CTkCheckBox(step_row, text="Parquet to CSV", variable=self.convert_var).pack(side="left")

        button_row = ctk.CTkFrame(form, fg_color="transparent")
        button_row.grid(row=8, column=0, columnspan=3, sticky="w", pady=(4, 2))
        self.run_button = ctk.CTkButton(
            button_row,
            text="Run pipeline",
            width=140,
            fg_color="#0a84ff",
            hover_color="#0071e3",
            command=self._run_pipeline,
        )
        self.run_button.pack(side="left")
        self.reset_button = ctk.CTkButton(
            button_row,
            text="Reset",
            width=90,
            fg_color="#1d1d1f",
            hover_color="#3a3a3c",
            command=self._reset_form,
        )
        self.reset_button.pack(side="left", padx=(12, 0))
        self.exit_button = ctk.CTkButton(
            button_row,
            text="Exit",
            width=90,
            fg_color="#1d1d1f",
            hover_color="#3a3a3c",
            command=self.destroy,
        )
        self.exit_button.pack(side="left", padx=(12, 0))

        progress_area = ctk.CTkFrame(card, fg_color="transparent")
        progress_area.pack(fill="x", padx=20, pady=(0, 8))
        ctk.CTkLabel(progress_area, text="Parsing progress", text_color="#1d1d1f").pack(anchor="w")
        self.parse_progress = ctk.CTkProgressBar(progress_area)
        self.parse_progress.pack(fill="x", pady=(6, 12))
        self.parse_progress.set(0)
        ctk.CTkLabel(progress_area, text="Conversion progress", text_color="#1d1d1f").pack(anchor="w")
        self.convert_progress = ctk.CTkProgressBar(progress_area)
        self.convert_progress.pack(fill="x", pady=(6, 6))
        self.convert_progress.set(0)

        paraview_row = ctk.CTkFrame(card, fg_color="transparent")
        paraview_row.pack(fill="x", padx=20, pady=(0, 8))
        ctk.CTkLabel(paraview_row, text="ParaView command", text_color="#1d1d1f").pack(anchor="w")
        pv_row = ctk.CTkFrame(paraview_row, fg_color="transparent")
        pv_row.pack(fill="x", pady=(6, 2))
        ctk.CTkEntry(pv_row, textvariable=self.paraview_path_var, width=360).pack(side="left")
        ctk.CTkButton(
            pv_row,
            text="Start ParaView",
            width=140,
            fg_color="#1d1d1f",
            hover_color="#3a3a3c",
            command=self._launch_paraview,
        ).pack(side="left", padx=(12, 0))

        log_area = ctk.CTkFrame(card, fg_color="transparent")
        log_area.pack(fill="both", expand=True, padx=20, pady=(0, 10))
        ctk.CTkLabel(log_area, text="Run log", text_color="#1d1d1f").pack(anchor="w")
        self.log_text = ctk.CTkTextbox(log_area, height=260)
        self.log_text.pack(fill="both", expand=True, pady=(6, 6))

    def _pick_file(self):
        path = filedialog.askopenfilename(
            title="Select UrQMD .dat file",
            initialdir=self.project_dir.parent,
            filetypes=[("UrQMD data", "*.dat")],
        )
        if path:
            self.input_file_var.set(path)

    def _progress_callback(self, stage, current, total, message=None):
        self.log_queue.put(("progress", stage, current, total, message))

    def _run_pipeline(self):
        event_str = self.event_number_var.get().strip()
        if not event_str.isdigit():
            self.log_text.insert(tk.END, "Event number must be a positive integer.\n")
            self.log_text.see(tk.END)
            return

        self.run_button.configure(state="disabled")
        self.log_text.delete("1.0", tk.END)
        self.parse_progress.set(0)
        self.convert_progress.set(0)

        def task():
            writer = LogWriter(self.log_queue)
            try:
                from urqmdvis.pipeline import run_pipeline
                with redirect_stdout(writer):
                    run_pipeline(
                        input_file=self.input_file_var.get().strip(),
                        event_number=int(event_str),
                        event_name=self.event_name_var.get().strip() or "event",
                        parquet_folder=self.parquet_folder_var.get().strip() or "output_parquet_files",
                        csv_folder=self.csv_folder_var.get().strip() or "output_csv_files",
                        run_reduce=self.reduce_var.get(),
                        run_parse=self.parse_var.get(),
                        run_convert=self.convert_var.get(),
                        progress_cb=self._progress_callback,
                    )
                self.log_queue.put(("status", "Pipeline finished."))
            except Exception as exc:
                self.log_queue.put(("status", f"Pipeline failed: {exc}"))
            finally:
                self.log_queue.put(("done",))

        threading.Thread(target=task, daemon=True).start()

    def _drain_queue(self):
        batched_lines = []
        processed = 0
        while True:
            try:
                item = self.log_queue.get_nowait()
            except queue.Empty:
                break
            processed += 1
            if processed > 200:
                break

            if item[0] == "log":
                batched_lines.append(item[1])
            elif item[0] == "progress":
                _, stage, current, total, _message = item
                if stage == "parse":
                    self._set_bar(self.parse_progress, current, total)
                elif stage == "convert":
                    self._set_bar(self.convert_progress, current, total)
            elif item[0] == "status":
                batched_lines.append(f"{item[1]}\n")
            elif item[0] == "done":
                self.run_button.configure(state="normal")

        if batched_lines:
            self.log_text.insert(tk.END, "".join(batched_lines))
            self.log_text.see(tk.END)

        self.after(50, self._drain_queue)

    def _set_bar(self, bar, current, total):
        if total and current is not None:
            bar.set(min(max(current / total, 0.0), 1.0))
        else:
            bar.set(0)

    def _reset_form(self):
        self.parse_progress.set(0)
        self.convert_progress.set(0)
        self.log_text.delete("1.0", tk.END)

    def _launch_paraview(self):
        cmd = self.paraview_path_var.get().strip()
        if not cmd:
            self.log_queue.put(("status", "ParaView command is empty."))
            return

        def starter():
            try:
                os.spawnlp(os.P_NOWAIT, cmd, cmd)
                self.log_queue.put(("status", f"Started ParaView: {cmd}"))
            except FileNotFoundError:
                self.log_queue.put(("status", f"ParaView not found: {cmd}"))
            except Exception as exc:
                self.log_queue.put(("status", f"Failed to start ParaView: {exc}"))

        threading.Thread(target=starter, daemon=True).start()


if __name__ == "__main__":
    app = UrqmdVisApp()
    app.mainloop()
