import os
import queue
import re
import shlex
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

        container = ctk.CTkFrame(self, fg_color="transparent")
        container.pack(fill="both", expand=True, padx=18, pady=(0, 0))

        header = ctk.CTkFrame(container, fg_color="#e0e0e4", corner_radius=16)
        header.pack(fill="x", padx=8, pady=(8, 4))
        header_inner = ctk.CTkFrame(header, fg_color="transparent")
        header_inner.pack(fill="x", padx=24, pady=6)
        header_inner.grid_columnconfigure(0, weight=1)
        header_inner.grid_columnconfigure(1, weight=0)

        title = ctk.CTkLabel(header_inner, text="urqmdVis", font=ctk.CTkFont(size=24, weight="bold"))
        subtitle = ctk.CTkLabel(
            header_inner,
            text="Local runner for UrQMD event visualization.",
            font=ctk.CTkFont(size=11),
            text_color="#1d1d1f",
        )
        title.grid(row=0, column=0, sticky="w")
        subtitle.grid(row=1, column=0, sticky="w")

        header_exit = ctk.CTkButton(
            header_inner,
            text="X",
            width=32,
            height=32,
            corner_radius=16,
            fg_color="#ff5f57",
            hover_color="#e14b45",
            text_color="#1d1d1f",
            command=self.destroy,
        )
        header_exit.grid(row=0, column=1, rowspan=2, sticky="e", padx=(0, 2))

        body = ctk.CTkScrollableFrame(container, fg_color="transparent", scrollbar_button_color="#d0d0d6")
        body.pack(fill="both", expand=True, padx=0, pady=(0, 6))

        card = ctk.CTkFrame(body, fg_color="#ffffff", corner_radius=18)
        card.pack(fill="both", expand=True, padx=0, pady=0)

        form = ctk.CTkFrame(card, fg_color="transparent")
        form.pack(fill="both", expand=True, padx=18, pady=(6, 2))

        pipeline_section = ctk.CTkFrame(form, fg_color="#f8f8fb", corner_radius=14)
        pipeline_section.pack(fill="x", pady=(0, 4))
        pipeline_inner = ctk.CTkFrame(pipeline_section, fg_color="transparent")
        pipeline_inner.pack(fill="x", padx=10, pady=4)

        viz_section = ctk.CTkFrame(form, fg_color="#f8f8fb", corner_radius=14)
        viz_section.pack(fill="x", pady=(0, 4))
        viz_inner = ctk.CTkFrame(viz_section, fg_color="transparent")
        viz_inner.pack(fill="x", padx=10, pady=4)

        self.input_file_var = tk.StringVar(value="../files/urqmd_AuAu_0-3fm/urqmd_1_14.dat")
        self.event_number_var = tk.StringVar(value="9")
        self.event_name_var = tk.StringVar(value="event")
        self.parquet_folder_var = tk.StringVar(value="outputs/parquet")
        self.csv_folder_var = tk.StringVar(value="outputs/csv")
        self.reduce_var = tk.BooleanVar(value=True)
        self.parse_var = tk.BooleanVar(value=True)
        self.convert_var = tk.BooleanVar(value=True)
        self.paraview_path_var = tk.StringVar(value="/Applications/ParaView-5.13.0.app/Contents/MacOS/paraview")
        self.b_value_var = tk.StringVar(value="0.0")
        self.sqrt_s_var = tk.StringVar(value="4.5")
        self.last_meta = None
        self.event_picker_var = tk.StringVar(value="")
        self.event_map = {}
        self.collision_system_var = tk.StringVar(value="Au--Au")

        ctk.CTkLabel(
            pipeline_inner,
            text="Pipeline",
            text_color="#1d1d1f",
            font=ctk.CTkFont(size=13, weight="bold"),
        ).grid(row=0, column=0, sticky="w")
        ctk.CTkLabel(pipeline_inner, text="Input .dat file", text_color="#1d1d1f").grid(row=1, column=0, sticky="w")
        pipeline_inner.grid_columnconfigure(0, weight=1)
        pipeline_inner.grid_columnconfigure(1, weight=0)
        pipeline_inner.grid_columnconfigure(2, weight=0)
        self.input_entry = ctk.CTkEntry(pipeline_inner, textvariable=self.input_file_var)
        self.input_entry.grid(row=2, column=0, columnspan=2, sticky="we", pady=(4, 10))
        ctk.CTkButton(
            pipeline_inner,
            text="Browse",
            width=90,
            fg_color="#1d1d1f",
            hover_color="#3a3a3c",
            command=self._pick_file,
        ).grid(row=2, column=2, padx=(8, 0), pady=(4, 10), sticky="e")

        ctk.CTkLabel(pipeline_inner, text="Event number", text_color="#1d1d1f").grid(row=3, column=0, sticky="w")
        self.event_spin = ctk.CTkEntry(pipeline_inner, textvariable=self.event_number_var, width=110)
        self.event_spin.grid(row=4, column=0, sticky="w", pady=(4, 8))

        ctk.CTkLabel(pipeline_inner, text="Event name prefix", text_color="#1d1d1f").grid(row=3, column=1, sticky="w")
        ctk.CTkEntry(pipeline_inner, textvariable=self.event_name_var, width=150).grid(row=4, column=1, sticky="w", pady=(4, 8))

        ctk.CTkLabel(pipeline_inner, text="Parquet output folder", text_color="#1d1d1f").grid(row=5, column=0, sticky="w")
        ctk.CTkEntry(pipeline_inner, textvariable=self.parquet_folder_var, width=190).grid(row=6, column=0, sticky="w", pady=(4, 8))

        ctk.CTkLabel(pipeline_inner, text="CSV output folder", text_color="#1d1d1f").grid(row=5, column=1, sticky="w")
        ctk.CTkEntry(pipeline_inner, textvariable=self.csv_folder_var, width=190).grid(row=6, column=1, sticky="w", pady=(4, 8))

        ctk.CTkLabel(pipeline_inner, text="Steps", text_color="#1d1d1f").grid(row=7, column=0, sticky="w")
        step_row = ctk.CTkFrame(pipeline_inner, fg_color="transparent")
        step_row.grid(row=8, column=0, columnspan=3, sticky="w", pady=(4, 8))
        ctk.CTkCheckBox(step_row, text="Reduce", variable=self.reduce_var).pack(side="left", padx=(0, 16))
        ctk.CTkCheckBox(step_row, text="Parse to Parquet", variable=self.parse_var).pack(side="left", padx=(0, 16))
        ctk.CTkCheckBox(step_row, text="Parquet to CSV", variable=self.convert_var).pack(side="left")

        button_row = ctk.CTkFrame(pipeline_inner, fg_color="transparent")
        button_row.grid(row=9, column=0, columnspan=3, sticky="we", pady=(4, 2))
        button_row.grid_columnconfigure(0, weight=1)
        button_row.grid_columnconfigure(1, weight=0)
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
        progress_area = ctk.CTkFrame(pipeline_inner, fg_color="transparent")
        progress_area.grid(row=10, column=0, columnspan=3, sticky="we", pady=(4, 4))
        ctk.CTkLabel(progress_area, text="Parsing progress", text_color="#1d1d1f").pack(anchor="w")
        self.parse_progress = ctk.CTkProgressBar(progress_area)
        self.parse_progress.pack(fill="x", pady=(4, 6))
        self.parse_progress.set(0)
        ctk.CTkLabel(progress_area, text="Conversion progress", text_color="#1d1d1f").pack(anchor="w")
        self.convert_progress = ctk.CTkProgressBar(progress_area)
        self.convert_progress.pack(fill="x", pady=(4, 0))
        self.convert_progress.set(0)

        ctk.CTkLabel(
            viz_inner,
            text="Visualization",
            text_color="#1d1d1f",
            font=ctk.CTkFont(size=13, weight="bold"),
        ).pack(anchor="w", pady=(0, 2))

        paraview_row = ctk.CTkFrame(viz_inner, fg_color="transparent")
        paraview_row.pack(fill="x", pady=(0, 8))
        pv_meta = ctk.CTkFrame(paraview_row, fg_color="transparent")
        pv_meta.pack(fill="x", pady=(4, 6))
        pv_meta.grid_columnconfigure(0, weight=1)
        pv_meta.grid_columnconfigure(1, weight=1)
        pv_meta.grid_columnconfigure(2, weight=1)
        pv_meta.grid_columnconfigure(3, weight=1)

        event_picker_frame = ctk.CTkFrame(pv_meta, fg_color="transparent")
        event_picker_frame.grid(row=0, column=0, sticky="w")
        ctk.CTkLabel(event_picker_frame, text="Event picker", text_color="#1d1d1f").pack(anchor="w")
        picker_row = ctk.CTkFrame(event_picker_frame, fg_color="transparent")
        picker_row.pack(anchor="w", pady=(4, 0))
        self.event_picker = ctk.CTkOptionMenu(
            picker_row,
            variable=self.event_picker_var,
            values=["Scan events"],
            command=self._on_event_pick,
            width=140,
        )
        self.event_picker.pack(side="left")
        ctk.CTkButton(
            picker_row,
            text="Refresh",
            width=70,
            fg_color="#1d1d1f",
            hover_color="#3a3a3c",
            command=self._scan_events,
        ).pack(side="left", padx=(6, 0))

        collision_frame = ctk.CTkFrame(pv_meta, fg_color="transparent")
        collision_frame.grid(row=0, column=1, sticky="w", padx=(16, 0))
        ctk.CTkLabel(collision_frame, text="Collision system", text_color="#1d1d1f").pack(anchor="w")
        ctk.CTkEntry(collision_frame, textvariable=self.collision_system_var, width=120).pack(anchor="w", pady=(4, 0))

        b_frame = ctk.CTkFrame(pv_meta, fg_color="transparent")
        b_frame.grid(row=0, column=2, sticky="w", padx=(16, 0))
        ctk.CTkLabel(b_frame, text="b (fm)", text_color="#1d1d1f").pack(anchor="w")
        ctk.CTkEntry(b_frame, textvariable=self.b_value_var, width=110).pack(anchor="w", pady=(4, 0))

        s_frame = ctk.CTkFrame(pv_meta, fg_color="transparent")
        s_frame.grid(row=0, column=3, sticky="w", padx=(16, 0))
        ctk.CTkLabel(s_frame, text="sqrt(s) (GeV)", text_color="#1d1d1f").pack(anchor="w")
        ctk.CTkEntry(s_frame, textvariable=self.sqrt_s_var, width=110).pack(anchor="w", pady=(4, 0))

        ctk.CTkLabel(paraview_row, text="ParaView command", text_color="#1d1d1f").pack(anchor="w")
        pv_row = ctk.CTkFrame(paraview_row, fg_color="transparent")
        pv_row.pack(fill="x", pady=(4, 2))
        pv_row.grid_columnconfigure(0, weight=1)
        pv_row.grid_columnconfigure(1, weight=0)
        pv_entry = ctk.CTkEntry(pv_row, textvariable=self.paraview_path_var)
        pv_entry.grid(row=0, column=0, sticky="we")
        ctk.CTkButton(
            pv_row,
            text="Start ParaView",
            width=140,
            fg_color="#1d1d1f",
            hover_color="#3a3a3c",
            command=self._launch_paraview,
        ).grid(row=0, column=1, padx=(12, 0), sticky="e")

        log_section = ctk.CTkFrame(form, fg_color="#f8f8fb", corner_radius=14)
        log_section.pack(fill="x", pady=(0, 4))
        log_inner = ctk.CTkFrame(log_section, fg_color="transparent")
        log_inner.pack(fill="x", padx=10, pady=4)
        ctk.CTkLabel(
            log_inner,
            text="Log",
            text_color="#1d1d1f",
            font=ctk.CTkFont(size=13, weight="bold"),
        ).pack(anchor="w")
        self.log_text = ctk.CTkTextbox(log_inner, height=130)
        self.log_text.pack(fill="both", expand=True, pady=(6, 6))

        self._scan_events()

    def _pick_file(self):
        path = filedialog.askopenfilename(
            title="Select UrQMD .dat file",
            initialdir=self.project_dir.parent,
            filetypes=[("UrQMD data", "*.dat")],
        )
        if path:
            self.input_file_var.set(path)

    def _scan_events(self):
        self.event_map = {}
        csv_folder_raw = self.csv_folder_var.get().strip() or "outputs/csv"
        csv_dir = Path(csv_folder_raw)
        if not csv_dir.is_absolute():
            csv_dir = (self.project_dir / csv_dir).resolve()
        candidate_dirs = [csv_dir]
        if not csv_dir.is_dir():
            candidate_dirs.extend(
                [
                    (self.project_dir / "outputs" / "csv").resolve(),
                    (self.project_dir / "outputs" / "output_csv_files").resolve(),
                    (self.project_dir / "output_csv_files").resolve(),
                ]
            )
        found_dir = next((d for d in candidate_dirs if d.is_dir()), None)
        if not found_dir:
            self.event_picker.configure(values=["No events found"])
            self.event_picker_var.set("No events found")
            self.log_queue.put(("status", f"Event picker: no CSV dir found for {csv_dir}"))
            return
        csv_dir = found_dir

        suffix_pattern = re.compile(r"^(?P<prefix>.*?)(?P<num>\d+)$")
        for path in csv_dir.glob("*.csv"):
            if "_frame_" not in path.name:
                continue
            left = path.name.split("_frame_", 1)[0]
            match = suffix_pattern.match(left)
            if not match:
                continue
            prefix = match.group("prefix")
            num = match.group("num")
            key = f"{prefix}{num}"
            self.event_map[key] = (prefix, num)

        options = sorted(self.event_map.keys(), key=lambda k: (k.rstrip("0123456789"), int(self.event_map[k][1])))
        if not options:
            self.event_picker.configure(values=["No events found"])
            self.event_picker_var.set("No events found")
            self.log_queue.put(("status", f"Event picker: no matches in {csv_dir}"))
            return

        self.event_picker.configure(values=options)
        current_key = f"{self.event_name_var.get().strip()}{self.event_number_var.get().strip()}"
        selected = current_key if current_key in self.event_map else options[0]
        self.event_picker_var.set(selected)
        self._on_event_pick(selected)
        self.log_queue.put(("status", f"Event picker: found {len(options)} events in {csv_dir}"))

    def _on_event_pick(self, value):
        if value in self.event_map:
            prefix, num = self.event_map[value]
            self.event_name_var.set(prefix)
            self.event_number_var.set(num)

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
                        parquet_folder=self.parquet_folder_var.get().strip() or "outputs/parquet",
                        csv_folder=self.csv_folder_var.get().strip() or "outputs/csv",
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
                _, stage, current, total, message = item
                if stage == "parse":
                    self._set_bar(self.parse_progress, current, total)
                elif stage == "convert":
                    self._set_bar(self.convert_progress, current, total)
                elif stage == "meta":
                    self.last_meta = message
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

    def _collision_system(self):
        if not self.last_meta:
            return "Unknown--Unknown"
        z_to_symbol = {
            1: "H", 2: "He", 3: "Li", 4: "Be", 5: "B", 6: "C", 7: "N", 8: "O", 9: "F", 10: "Ne",
            11: "Na", 12: "Mg", 13: "Al", 14: "Si", 15: "P", 16: "S", 17: "Cl", 18: "Ar", 19: "K", 20: "Ca",
            21: "Sc", 22: "Ti", 23: "V", 24: "Cr", 25: "Mn", 26: "Fe", 27: "Co", 28: "Ni", 29: "Cu", 30: "Zn",
            31: "Ga", 32: "Ge", 33: "As", 34: "Se", 35: "Br", 36: "Kr", 37: "Rb", 38: "Sr", 39: "Y", 40: "Zr",
            41: "Nb", 42: "Mo", 43: "Tc", 44: "Ru", 45: "Rh", 46: "Pd", 47: "Ag", 48: "Cd", 49: "In", 50: "Sn",
            51: "Sb", 52: "Te", 53: "I", 54: "Xe", 55: "Cs", 56: "Ba", 57: "La", 58: "Ce", 59: "Pr", 60: "Nd",
            61: "Pm", 62: "Sm", 63: "Eu", 64: "Gd", 65: "Tb", 66: "Dy", 67: "Ho", 68: "Er", 69: "Tm", 70: "Yb",
            71: "Lu", 72: "Hf", 73: "Ta", 74: "W", 75: "Re", 76: "Os", 77: "Ir", 78: "Pt", 79: "Au", 80: "Hg",
            81: "Tl", 82: "Pb", 83: "Bi", 84: "Po", 85: "At", 86: "Rn", 87: "Fr", 88: "Ra", 89: "Ac", 90: "Th",
            91: "Pa", 92: "U", 93: "Np", 94: "Pu", 95: "Am", 96: "Cm", 97: "Bk", 98: "Cf", 99: "Es", 100: "Fm",
            101: "Md", 102: "No", 103: "Lr", 104: "Rf", 105: "Db", 106: "Sg", 107: "Bh", 108: "Hs", 109: "Mt",
            110: "Ds", 111: "Rg", 112: "Cn", 113: "Nh", 114: "Fl", 115: "Mc", 116: "Lv", 117: "Ts", 118: "Og",
        }
        z_a = self.last_meta.get("chA")
        z_b = self.last_meta.get("chB")
        a_a = self.last_meta.get("massA")
        a_b = self.last_meta.get("massB")
        sym_a = z_to_symbol.get(int(z_a)) if z_a is not None else None
        sym_b = z_to_symbol.get(int(z_b)) if z_b is not None else None
        left = sym_a or "Unknown"
        right = sym_b or "Unknown"
        return f"{left}--{right}"

    def _launch_paraview(self):
        cmd = self.paraview_path_var.get().strip()
        b_value = self.b_value_var.get().strip()
        sqrt_s = self.sqrt_s_var.get().strip()
        if not cmd:
            self.log_queue.put(("status", "ParaView command is empty."))
            return
        if not b_value:
            self.log_queue.put(("status", "Impact parameter b is empty."))
            return
        if not sqrt_s:
            self.log_queue.put(("status", "sqrt(s) is empty."))
            return

        def starter():
            try:
                from urqmdvis.paraview_state import generate_state

                csv_folder_raw = self.csv_folder_var.get().strip() or "outputs/csv"
                csv_dir = Path(csv_folder_raw)
                if not csv_dir.is_absolute():
                    csv_dir = (self.project_dir / csv_dir).resolve()
                selected = self.event_picker_var.get().strip()
                if selected in self.event_map:
                    prefix_name, prefix_num = self.event_map[selected]
                else:
                    prefix_name = self.event_name_var.get().strip()
                    prefix_num = self.event_number_var.get().strip()
                prefix = f"{prefix_name}{prefix_num}_frame_"
                frame_pattern = re.compile(r"_frame_(\d+)\.csv$")
                unsorted = list(csv_dir.glob(f"{prefix}*.csv"))
                csv_files = sorted(
                    unsorted,
                    key=lambda p: int(frame_pattern.search(p.name).group(1)) if frame_pattern.search(p.name) else -1,
                )
                if not csv_files:
                    self.log_queue.put(("status", f"No CSV files found in {csv_dir} for prefix {prefix}"))
                    return

                scale = self.last_meta.get("dtime", 1.0) if self.last_meta else 1.0
                collision = self.collision_system_var.get().strip() or self._collision_system()
                text_block = f"UrQMD v3.4\n{collision} (b = {b_value} fm)\nsqrt(s): {sqrt_s} GeV"
                state_path = self.project_dir / "outputs" / "paraview" / f"{prefix}state.pvsm"
                generate_state(
                    template_path=self.project_dir / "assets" / "paraview" / "paraViewState-massVar.pvsm",
                    output_path=state_path,
                    csv_files=csv_files,
                    scale=scale,
                    text_block=text_block,
                )

                args = shlex.split(cmd)
                os.spawnv(os.P_NOWAIT, args[0], args + ["--state", str(state_path)])
                self.log_queue.put(("status", f"Started ParaView with state: {state_path}"))
                self.log_queue.put(("status", f"CSV sequence: {csv_files[0]} ..."))
            except FileNotFoundError:
                self.log_queue.put(("status", f"ParaView not found: {cmd}"))
            except Exception as exc:
                self.log_queue.put(("status", f"Failed to start ParaView: {exc}"))

        threading.Thread(target=starter, daemon=True).start()


if __name__ == "__main__":
    app = UrqmdVisApp()
    app.mainloop()
