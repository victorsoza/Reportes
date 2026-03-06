from typing import Optional
import os
import logging

from PyQt6.QtWidgets import QDialog, QLabel, QVBoxLayout, QApplication, QWidget
from PyQt6.QtGui import QMovie
from PyQt6 import QtCore
import sys


class LoadingDialog:
    """Diálogo reutilizable de carga.

    Uso:
        ld = LoadingDialog(parent)
        ld.show("Cargando...")
        ld.hide()
    """

    def __init__(self, parent: QWidget, default_text: str = "Cargando...", gif_path: Optional[str] = None, modal: bool = False, logger: Optional[logging.Logger] = None):
        self.parent = parent
        self.default_text = default_text
        self._dialog: Optional[QDialog] = None
        self._movie: Optional[QMovie] = None
        self._gif_path = gif_path or self.find_loading_gif()
        self._modal = modal
        self._logger: logging.Logger = logger or logging.getLogger(__name__)
        self._refcount = 0

    def show(self, text: Optional[str] = None, gif_path: Optional[str] = None) -> None:
        """Muestra el diálogo; crea la instancia si es necesario."""
        try:
            if self._dialog is not None:
                try:
                    lbl = self._dialog.findChild(QLabel, "loading_label")
                    if lbl is not None and text:
                        lbl.setText(text)
                    self._dialog.show()
                    return
                except Exception:
                    # si algo falla, recreamos
                    try:
                        self._logger.debug("LoadingDialog: fallo actualizando diálogo existente, recreando", exc_info=True)
                        self.hide()
                    except Exception:
                        self._logger.debug("LoadingDialog: hide() falló durante la recuperación", exc_info=True)

            # Increment refcount and ensure dialog remains until corresponding hides
            self._refcount = (self._refcount or 0) + 1
            dlg = QDialog(self.parent)
            dlg.setWindowFlags(QtCore.Qt.WindowType.FramelessWindowHint | QtCore.Qt.WindowType.Dialog)
            try:
                dlg.setAttribute(QtCore.Qt.WidgetAttribute.WA_TranslucentBackground, True)
            except Exception:
                self._logger.debug("LoadingDialog: no se pudo establecer WA_TranslucentBackground", exc_info=True)
            try:
                dlg.setStyleSheet("background: transparent;")
            except Exception:
                self._logger.debug("LoadingDialog: fallo aplicando styleSheet al diálogo", exc_info=True)
            dlg.setModal(self._modal)
            layout = QVBoxLayout(dlg)
            layout.setContentsMargins(12, 12, 12, 12)

            label = QLabel(dlg)
            label.setObjectName("loading_label")
            try:
                label.setStyleSheet("background: transparent;")
            except Exception:
                self._logger.debug("LoadingDialog: fallo aplicando styleSheet al label", exc_info=True)

            movie = None
            path = gif_path or self._gif_path
            try:
                if path and os.path.exists(path):
                    movie = QMovie(path)
                    label.setMovie(movie)
                    movie.start()
                else:
                    label.setText(text or self.default_text)
                    label.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)
            except Exception:
                self._logger.debug("LoadingDialog: fallo cargando GIF, mostrando texto en su lugar", exc_info=True)
                label.setText(text or self.default_text)
                label.setAlignment(QtCore.Qt.AlignmentFlag.AlignCenter)

            layout.addWidget(label)

            dlg.adjustSize()
            try:
                parent_rect = self.parent.geometry()
                global_pos = self.parent.mapToGlobal(parent_rect.topLeft())
                x = global_pos.x() + (parent_rect.width() - dlg.width()) // 2
                y = global_pos.y() + (parent_rect.height() - dlg.height()) // 2
                dlg.move(int(x), int(y))
            except Exception:
                self._logger.debug("LoadingDialog: no se pudo centrar el diálogo respecto al parent", exc_info=True)

            dlg.show()
            try:
                dlg.raise_()
                dlg.activateWindow()
            except Exception:
                self._logger.debug("LoadingDialog: fallo al intentar activar/elevar ventana", exc_info=True)
            try:
                QApplication.processEvents()
            except Exception:
                self._logger.debug("LoadingDialog: processEvents falló", exc_info=True)

            self._dialog = dlg
            self._movie = movie
        except Exception:
            # no debe lanzar desde el hilo UI
            self._logger.exception("LoadingDialog: error mostrando el diálogo")
        except Exception:
            # no debe lanzar desde el hilo UI
            self._logger.exception("LoadingDialog: error mostrando el diálogo")

    def hide(self) -> None:
        try:
            # Decrement refcount; solo cerrar si llega a 0
            try:
                self._refcount = max(0, (self._refcount or 1) - 1)
            except Exception:
                self._refcount = 0
            if self._refcount > 0:
                return
            if self._dialog is not None:
                try:
                    self._dialog.close()
                except Exception:
                    self._logger.debug("LoadingDialog: close() falló", exc_info=True)
                try:
                    if self._movie is not None:
                        try:
                            self._movie.stop()
                        except Exception:
                            self._logger.debug("LoadingDialog: movie.stop() falló", exc_info=True)
                        self._movie = None
                except Exception:
                    self._logger.debug("LoadingDialog: fallo gestionando movie durante hide()", exc_info=True)
                self._dialog = None
        except Exception:
            self._logger.exception("LoadingDialog: error en hide()")

    @staticmethod
    def find_loading_gif() -> Optional[str]:
        try:
            # Considerar ruta en PyInstaller bundle
            base = getattr(sys, '_MEIPASS', None) or os.path.dirname(__file__)
            cur = base
            candidate = os.path.join(cur, 'loading.gif')
            if os.path.exists(candidate):
                return os.path.abspath(candidate)
            for _ in range(6):
                candidate = os.path.join(cur, 'Iconos', 'loading.gif')
                if os.path.exists(candidate):
                    return os.path.abspath(candidate)
                candidate2 = os.path.join(cur, 'loading.gif')
                if os.path.exists(candidate2):
                    return os.path.abspath(candidate2)
                parent = os.path.dirname(cur)
                if not parent or parent == cur:
                    break
                cur = parent
        except Exception:
            logging.getLogger(__name__).debug("find_loading_gif falló", exc_info=True)
        return None


def get_loading_dialog(parent: QWidget, **kwargs) -> LoadingDialog:
    """Factory para reutilizar una instancia de LoadingDialog por parent.

    Guarda la instancia en el atributo privado `_shared_loading_dialog` del parent.
    """
    try:
        existing = getattr(parent, '_shared_loading_dialog', None)
        if existing is None:
            dlg = LoadingDialog(parent, **kwargs)
            try:
                setattr(parent, '_shared_loading_dialog', dlg)
            except Exception:
                pass
            return dlg
        return existing
    except Exception:
        return LoadingDialog(parent, **kwargs)
