# -*- coding: utf-8 -*-
"""
Subtitle Generator - Genera subtítulos SRT desde transcripciones de WhisperX

Este módulo toma la transcripción con timestamps y la convierte a formato SRT.
Los subtítulos pueden quemarse en el video (hard-coded) o agregarse como pista (soft-coded).
"""

import re
import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from rich.console import Console

from src.utils.logger import get_logger

logger = get_logger(__name__)


class SubtitleGenerator:
    """
    Genero subtítulos en formato SRT desde transcripciones de WhisperX

    Características:
    - Formato SRT estándar (compatible con todos los players)
    - Agrupación inteligente de palabras con múltiples presets
    - Sincronización perfecta con timestamps de WhisperX
    - Soporte para múltiples idiomas

    Presets de agrupación:
    - smart: Usa todas las señales (pausas, oraciones, cláusulas, conjunciones)
    - punctuation: Solo corta en puntuación (oraciones y comas)
    - pauses: Solo corta en pausas del habla
    - fixed: Comportamiento original, solo por conteo de caracteres y duración
    """

    # Presets de agrupación con sus parámetros
    GROUPING_PRESETS: Dict[str, Dict] = {
        "smart": {
            "description": "Intelligent splitting using pauses, punctuation, and clauses",
            "split_on_pause": True,
            "split_on_sentence_end": True,
            "split_on_clause_end": True,
            "split_on_clause_start": True,
            "pause_threshold": 0.7,
        },
        "punctuation": {
            "description": "Split only on punctuation marks (periods, commas, etc.)",
            "split_on_pause": False,
            "split_on_sentence_end": True,
            "split_on_clause_end": True,
            "split_on_clause_start": False,
            "pause_threshold": 0.7,
        },
        "pauses": {
            "description": "Split only on speech pauses (silence gaps)",
            "split_on_pause": True,
            "split_on_sentence_end": False,
            "split_on_clause_end": False,
            "split_on_clause_start": False,
            "pause_threshold": 0.7,
        },
        "fixed": {
            "description": "Simple character count and duration limits (no smart splitting)",
            "split_on_pause": False,
            "split_on_sentence_end": False,
            "split_on_clause_end": False,
            "split_on_clause_start": False,
            "pause_threshold": 0.7,
        },
    }

    def __init__(self):
        self.console = Console()
        self.logger = logger

    @classmethod
    def get_grouping_presets(cls) -> Dict[str, Dict]:
        """Retorno los presets de agrupación disponibles con sus descripciones."""
        return {
            name: {"description": preset["description"], **preset}
            for name, preset in cls.GROUPING_PRESETS.items()
        }

    def generate_srt_from_transcript(
        self,
        transcript_path: str,
        output_path: Optional[str] = None,
        max_chars_per_line: int = 42,
        max_duration: float = 5.0,
        grouping_preset: str = "smart",
        pause_threshold: Optional[float] = None,
    ) -> Optional[str]:
        """
        Genero archivo SRT desde transcripción de WhisperX

        Args:
            transcript_path: Ruta al JSON de transcripción
            output_path: Ruta de salida para el SRT (opcional)
            max_chars_per_line: Máximo de caracteres por línea de subtítulo
            max_duration: Duración máxima de un subtítulo en segundos
            grouping_preset: Preset de agrupación (smart, punctuation, pauses, fixed)
            pause_threshold: Umbral de pausa en segundos (sobrescribe el del preset)

        Returns:
            Ruta al archivo SRT generado, o None si falla
        """
        try:
            # Cargo la transcripción
            with open(transcript_path, 'r', encoding='utf-8') as f:
                transcript_data = json.load(f)

            segments = transcript_data.get('segments', [])

            if not segments:
                self.logger.error("No se encontraron segmentos en la transcripción")
                return None

            # Genero el path de salida si no se especificó
            if output_path is None:
                transcript_file = Path(transcript_path)
                output_path = transcript_file.with_suffix('.srt')

            # Genero las entradas SRT
            srt_entries = self._create_srt_entries(
                segments,
                max_chars_per_line=max_chars_per_line,
                max_duration=max_duration,
                grouping_preset=grouping_preset,
                pause_threshold=pause_threshold,
            )

            # Escribo el archivo SRT
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write('\n'.join(srt_entries))

            self.logger.info(f"Subtítulos generados: {output_path}")
            return str(output_path)

        except Exception as e:
            self.logger.error(f"Error generando subtítulos: {e}")
            return None


    def generate_srt_for_clip(
        self,
        transcript_path: str,
        clip_start: float,
        clip_end: float,
        output_path: str,
        max_chars_per_line: int = 42,
        max_duration: float = 5.0,
        grouping_preset: str = "smart",
        pause_threshold: Optional[float] = None,
    ) -> Optional[str]:
        """
        Genero archivo SRT para un clip específico

        Args:
            transcript_path: Ruta al JSON de transcripción completa
            clip_start: Tiempo de inicio del clip en segundos
            clip_end: Tiempo de fin del clip en segundos
            output_path: Ruta de salida para el SRT
            max_chars_per_line: Máximo de caracteres por línea
            max_duration: Duración máxima de un subtítulo

        Returns:
            Ruta al archivo SRT generado, o None si falla
        """
        try:
            # Cargo la transcripción
            with open(transcript_path, 'r', encoding='utf-8') as f:
                transcript_data = json.load(f)

            segments = transcript_data.get('segments', [])

            # Filtro solo los segmentos que están dentro del clip
            clip_segments = []
            for segment in segments:
                seg_start = segment.get('start', 0)
                seg_end = segment.get('end', 0)

                # Si el segmento se solapa con el clip, lo incluyo
                if seg_start < clip_end and seg_end > clip_start:
                    # Ajusto los timestamps relativos al inicio del clip
                    adjusted_segment = segment.copy()

                    # Ajusto palabras si existen
                    if 'words' in segment:
                        adjusted_words = []
                        for word in segment['words']:
                            word_start = word.get('start', 0)
                            word_end = word.get('end', 0)

                            # Solo incluyo palabras dentro del rango del clip
                            if word_start >= clip_start and word_end <= clip_end:
                                adjusted_word = word.copy()
                                adjusted_word['start'] = word_start - clip_start
                                adjusted_word['end'] = word_end - clip_start
                                adjusted_words.append(adjusted_word)

                        adjusted_segment['words'] = adjusted_words

                    # Ajusto los timestamps del segmento
                    adjusted_segment['start'] = max(0, seg_start - clip_start)
                    adjusted_segment['end'] = min(clip_end - clip_start, seg_end - clip_start)

                    clip_segments.append(adjusted_segment)

            if not clip_segments:
                self.logger.warning(f"No se encontraron segmentos para el clip {clip_start}-{clip_end}")
                return None

            # Genero las entradas SRT
            srt_entries = self._create_srt_entries(
                clip_segments,
                max_chars_per_line=max_chars_per_line,
                max_duration=max_duration,
                grouping_preset=grouping_preset,
                pause_threshold=pause_threshold,
            )

            # Escribo el archivo SRT
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write('\n'.join(srt_entries))

            self.logger.info(f"Subtítulos del clip generados: {output_path}")
            return str(output_path)

        except Exception as e:
            self.logger.error(f"Error generando subtítulos del clip: {e}")
            return None


    # Palabras que inician cláusulas — preferimos cortar ANTES de ellas
    _CLAUSE_STARTERS = frozenset({
        'and', 'but', 'or', 'so', 'because', 'since', 'while', 'when',
        'where', 'which', 'that', 'then', 'if', 'although', 'though',
        'however', 'also', 'plus', 'yet', 'nor',
        # Spanish
        'y', 'pero', 'o', 'porque', 'cuando', 'donde', 'que', 'entonces',
        'si', 'aunque', 'también', 'ni', 'sino',
    })

    # Puntuación que marca fin de oración — corte obligatorio después
    _SENTENCE_END_RE = re.compile(r'[.!?]$')

    # Puntuación que marca pausa de cláusula — corte preferido después
    _CLAUSE_END_RE = re.compile(r'[,;:\u2014—\-]$')

    # Umbral de pausa entre palabras para forzar corte (segundos)
    _PAUSE_THRESHOLD = 0.7

    # Mínimo de palabras antes de permitir un corte por puntuación/pausa
    _MIN_WORDS_PER_LINE = 2

    def _create_srt_entries(
        self,
        segments: List[Dict],
        max_chars_per_line: int = 42,
        max_duration: float = 5.0,
        grouping_preset: str = "smart",
        pause_threshold: Optional[float] = None,
    ) -> List[str]:
        """
        Creo las entradas en formato SRT desde segmentos con agrupación inteligente.

        Usa múltiples señales para decidir dónde cortar (según el preset):
        1. Pausas en el habla (gap > pause_threshold entre palabras)
        2. Fin de oración (. ! ?)
        3. Fin de cláusula (, ; :) y conjunciones (but, and, so...)
        4. Límites duros de caracteres y duración como respaldo

        Formato SRT:
        1
        00:00:00,000 --> 00:00:03,500
        This is the first subtitle line

        2
        00:00:03,500 --> 00:00:07,000
        This is the second subtitle line
        """
        # Resuelvo configuración del preset
        preset = self.GROUPING_PRESETS.get(grouping_preset, self.GROUPING_PRESETS["smart"])
        split_on_pause = preset["split_on_pause"]
        split_on_sentence = preset["split_on_sentence_end"]
        split_on_clause = preset["split_on_clause_end"]
        split_on_clause_start = preset["split_on_clause_start"]
        effective_pause = pause_threshold if pause_threshold is not None else preset["pause_threshold"]

        srt_entries = []
        subtitle_index = 1

        for segment in segments:
            # Uso palabras si están disponibles (mejor sincronización)
            if 'words' in segment and segment['words']:
                words = segment['words']

                # Filtro palabras vacías y preparo lista limpia
                clean_words = []
                for w in words:
                    text = w.get('word', '').strip()
                    if text:
                        clean_words.append(w)

                if not clean_words:
                    continue

                # Agrupo palabras inteligentemente
                current_line_words = []
                current_line_chars = 0
                line_start_time = None

                def _flush_line():
                    nonlocal current_line_words, current_line_chars, line_start_time
                    nonlocal subtitle_index
                    if not current_line_words:
                        return
                    line_text = ' '.join(
                        w.get('word', '').strip() for w in current_line_words
                    )
                    line_end_time = current_line_words[-1].get('end', line_start_time + 1.0)
                    srt_entry = self._format_srt_entry(
                        subtitle_index, line_start_time, line_end_time, line_text
                    )
                    srt_entries.append(srt_entry)
                    subtitle_index += 1
                    current_line_words = []
                    current_line_chars = 0
                    line_start_time = None

                for i, word_obj in enumerate(clean_words):
                    word_text = word_obj.get('word', '').strip()
                    word_start = word_obj.get('start', 0)
                    word_end = word_obj.get('end', 0)

                    # Inicio de nueva línea
                    if line_start_time is None:
                        line_start_time = word_start

                    word_length = len(word_text) + 1  # +1 por el espacio

                    # --- Detección de pausa entre esta palabra y la anterior ---
                    has_pause = False
                    if split_on_pause and current_line_words:
                        prev_end = current_line_words[-1].get('end', 0)
                        gap = word_start - prev_end
                        has_pause = gap >= effective_pause

                    # --- Verifico límites duros ---
                    exceeds_chars = current_line_chars + word_length > max_chars_per_line
                    exceeds_duration = (
                        line_start_time is not None
                        and word_end - line_start_time > max_duration
                    )

                    # --- Pausa larga fuerza corte ---
                    pause_forces_break = (
                        has_pause
                        and len(current_line_words) >= self._MIN_WORDS_PER_LINE
                    )

                    if exceeds_chars or exceeds_duration or pause_forces_break:
                        _flush_line()
                        current_line_words = [word_obj]
                        current_line_chars = word_length
                        line_start_time = word_start
                    else:
                        # Agrego palabra a la línea actual
                        current_line_words.append(word_obj)
                        current_line_chars += word_length

                        # --- Corte proactivo en límites naturales ---
                        if len(current_line_words) >= self._MIN_WORDS_PER_LINE:
                            should_flush = False

                            # Fin de oración: corte inmediato
                            if split_on_sentence and self._SENTENCE_END_RE.search(word_text):
                                should_flush = True

                            # Fin de cláusula (coma, punto y coma, etc.)
                            elif split_on_clause and self._CLAUSE_END_RE.search(word_text):
                                should_flush = True

                            # La siguiente palabra es un inicio de cláusula
                            elif split_on_clause_start and (i + 1 < len(clean_words)):
                                next_word = clean_words[i + 1].get('word', '').strip().lower()
                                next_clean = re.sub(r'[^\w]', '', next_word)
                                if next_clean in self._CLAUSE_STARTERS:
                                    should_flush = True

                            if should_flush:
                                _flush_line()

                # Proceso última línea si quedó algo
                _flush_line()

            else:
                # Fallback: uso el texto completo del segmento
                text = segment.get('text', '').strip()
                start = segment.get('start', 0)
                end = segment.get('end', 0)

                if text:
                    # Divido texto largo en líneas
                    lines = self._split_text_into_lines(text, max_chars_per_line)
                    duration = end - start
                    time_per_line = duration / len(lines) if lines else duration

                    for i, line in enumerate(lines):
                        line_start = start + (i * time_per_line)
                        line_end = start + ((i + 1) * time_per_line)

                        srt_entry = self._format_srt_entry(
                            subtitle_index,
                            line_start,
                            line_end,
                            line
                        )
                        srt_entries.append(srt_entry)
                        subtitle_index += 1

        return srt_entries


    def _format_srt_entry(
        self,
        index: int,
        start_time: float,
        end_time: float,
        text: str
    ) -> str:
        """
        Formateo una entrada SRT

        Args:
            index: Número de subtítulo
            start_time: Tiempo de inicio en segundos
            end_time: Tiempo de fin en segundos
            text: Texto del subtítulo

        Returns:
            String con formato SRT
        """
        start_str = self._seconds_to_srt_time(start_time)
        end_str = self._seconds_to_srt_time(end_time)

        return f"{index}\n{start_str} --> {end_str}\n{text}\n"


    def _seconds_to_srt_time(self, seconds: float) -> str:
        """
        Convierto segundos a formato SRT (HH:MM:SS,mmm)

        Args:
            seconds: Tiempo en segundos

        Returns:
            String en formato SRT
        """
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = int(seconds % 60)
        millis = int((seconds % 1) * 1000)

        return f"{hours:02d}:{minutes:02d}:{secs:02d},{millis:03d}"


    def _split_text_into_lines(self, text: str, max_chars: int) -> List[str]:
        """
        Divido texto largo en líneas respetando límite de caracteres

        Intenta cortar en espacios para no partir palabras

        Args:
            text: Texto a dividir
            max_chars: Máximo de caracteres por línea

        Returns:
            Lista de líneas
        """
        words = text.split()
        lines = []
        current_line = []
        current_length = 0

        for word in words:
            word_length = len(word) + 1  # +1 por el espacio

            if current_length + word_length > max_chars:
                if current_line:
                    lines.append(' '.join(current_line))
                current_line = [word]
                current_length = len(word)
            else:
                current_line.append(word)
                current_length += word_length

        # Agrego última línea
        if current_line:
            lines.append(' '.join(current_line))

        return lines if lines else [text]
