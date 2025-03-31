import PointWidget
import RouteWidget

from u8 import Arc
import sys
from PyQt5 import QtCore, QtWidgets, QtGui

Qt = QtCore.Qt


class MainWindow(QtWidgets.QMainWindow):
    def __init__(self):
        super(MainWindow, self).__init__()
        self.setWindowTitle('RouteEdit')
        self.setGeometry(500, 500, 1500, 750)

        self.saveFile = QtWidgets.QAction(QtGui.QIcon('RouteEditData/icons/save.png'), '&Save', self)
        self.saveAsFile = QtWidgets.QAction(QtGui.QIcon('RouteEditData/icons/saveAs.png'), '&Save As', self)
        self.openFile = QtWidgets.QAction(QtGui.QIcon('RouteEditData/icons/folder.png'), '&Open', self)
        self.closeFile = QtWidgets.QAction(QtGui.QIcon('RouteEditData/icons/close.png'), '&Close', self)

        self.editor = EditorTabWidget()

        self.initUi()

        self.currentFilePath = ''

    def initUi(self):

        self.editor.setDisabled(True)

        # setup menu bar
        mainMenu = self.menuBar()
        fileMenu = mainMenu.addMenu('&File')
        toolBar = self.addToolBar('File')

        toolBar.setMovable(False)

        self.openFile.setShortcut('Ctrl+O')
        self.saveFile.setShortcut('Ctrl+S')
        self.saveAsFile.setShortcut('Ctrl+Shift+S')
        self.closeFile.setShortcut('Ctrl+W')

        self.openFile.setStatusTip('Open a file')
        self.saveFile.setStatusTip('Save Changes')
        self.saveAsFile.setStatusTip('Save As')
        self.closeFile.setStatusTip('Close the current file')

        self.openFile.triggered.connect(self.loadArc)
        self.saveFile.triggered.connect(self.saveArc)
        self.saveAsFile.triggered.connect(self.saveSarcAs)
        self.closeFile.triggered.connect(self.closeSarc)

        self.saveFile.setDisabled(True)
        self.saveAsFile.setDisabled(True)
        self.closeFile.setDisabled(True)

        fileMenu.addAction(self.openFile)
        fileMenu.addAction(self.saveFile)
        fileMenu.addAction(self.saveAsFile)
        fileMenu.addAction(self.closeFile)

        toolBar.addAction(self.openFile)
        toolBar.addSeparator()
        toolBar.addAction(self.saveFile)
        toolBar.addAction(self.saveAsFile)
        toolBar.addSeparator()
        toolBar.addAction(self.closeFile)

        self.setCentralWidget(self.editor)

    def loadArc(self):
        fileName = QtWidgets.QFileDialog.getOpenFileName(self, 'Open file', '', 'ARC files (*.arc)')[0]

        if fileName == '':
            return

        self.currentFilePath = fileName
        archive = Arc.from_file(fileName)

        self.editor.loadData(archive)

        self.saveFile.setDisabled(False)
        self.saveAsFile.setDisabled(False)
        self.closeFile.setDisabled(False)
        self.editor.setDisabled(False)

    def saveArc(self):
        arcContents = self.editor.getDataFromWidgets()
        arcContents.sort(key=lambda __f: __f.name)

        newArchive = Arc()

        for file in arcContents:
            folder_name = file.name[5:-4]
            if len(folder_name) > 2:
                folder_name = folder_name[:2]

            newArchive.append_file(file.name, file.data, path=folder_name + '/' + file.name)

        with open(self.currentFilePath, 'wb+') as f:
            f.write(newArchive.to_bytes())

    def saveSarcAs(self):
        arcContents = self.editor.getDataFromWidgets()
        arcContents.sort(key=lambda __f: __f.name)

        newArchive = Arc()

        for file in arcContents:
            folder_name = file.name[5:-4]
            if len(folder_name) > 2:
                folder_name = folder_name[:2]

            newArchive.append_file(file.name, file.data, path=folder_name + '/' + file.name)

        fileName = QtWidgets.QFileDialog.getSaveFileName(self, 'Save file', '', 'ARC files (*.arc)')[0]

        if fileName == '':
            return

        with open(fileName, 'wb+') as f:
            f.write(newArchive.to_bytes())

    def closeSarc(self):
        closeDialog = QtWidgets.QMessageBox
        ret = closeDialog.question(self, '', 'Close the current file?', closeDialog.Yes | closeDialog.No)

        if ret == closeDialog.Yes:
            self.editor.closeFile()
            self.editor.setDisabled(True)
            self.saveFile.setDisabled(True)
            self.saveAsFile.setDisabled(True)
            self.closeFile.setDisabled(True)

            self.currentFilePath = ''


class EditorTabWidget(QtWidgets.QTabWidget):
    def __init__(self, parent=None):
        QtWidgets.QTabWidget.__init__(self, parent)

        self.pointEditor = PointWidget.PointEditorWidget()
        self.routeEditor = RouteWidget.RouteEditorWidget()
        self.addTab(self.pointEditor, 'Node Unlocks')
        self.addTab(self.routeEditor, 'Path Settings')

    def loadData(self, archiveContents):
        self.closeFile()

        pointFiles = []
        routeFiles = []

        # Keep the order. We load them in lazily, so we do not
        # preserve the original order of the u8 archive.
        files = sorted(list(archiveContents.get_all_files().values()), key=lambda __f: __f.name)
        for f in files:
            if f.name.startswith('point'):
                pointFiles.append(f)
            elif f.name.startswith('route'):
                routeFiles.append(f)
            else:
                print('Unknown File')
                print(f)

        self.pointEditor.loadData(pointFiles)
        self.routeEditor.loadData(routeFiles)

    def closeFile(self):
        self.pointEditor.closeData()
        self.routeEditor.closeData()

    def getDataFromWidgets(self):
        pointFiles = self.pointEditor.getArchiveContents()
        routeFiles = self.routeEditor.getArchiveContents()

        archiveContents = pointFiles + routeFiles

        return archiveContents


if __name__ == '__main__':
    app = QtWidgets.QApplication(sys.argv)
    window = MainWindow()
    window.show()

    sys.exit(app.exec_())
