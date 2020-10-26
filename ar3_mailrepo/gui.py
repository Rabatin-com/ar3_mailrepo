#!/usr/bin/env python
"""
Hello World, but with more meat.
"""

import platform
from pathlib import Path

import wx
import wx.grid

import ar3_mailrepo_config
import ar3_mailrepo_lib
import ar3_mailrepo_version_info
import util_lib

ID_GMAIL = wx.NewId()
ID_OUTLOOK = wx.NewId()


def populate_grid_with_dict_list(gridobj, dictlist):
  gridobj.DeleteRows(0, gridobj.GetNumberRows())
  gridobj.DeleteCols(0, gridobj.GetNumberCols())
  headers = set()
  for folder_labels in dictlist:
    headers.update(folder_labels)
  headers_list = list(headers)

  gridobj.AppendRows(max(len(dictlist), 1))
  gridobj.AppendCols(max(len(headers_list), 1))

  for hdr_ix, hdr in enumerate(headers_list):
    gridobj.SetColLabelValue(hdr_ix, hdr.capitalize())

  for ix, folder in enumerate(dictlist):
    for folderkey, folderdata in folder.items():
      gridobj.SetCellValue(ix, headers_list.index(folderkey), str(folderdata))

  gridobj.AutoSizeColumns()


class EmailFolderListingTab(wx.Panel):

  @staticmethod
  def add_as_page(parent, credentials_root: Path):
    tab = EmailFolderListingTab(parent, credentials_root)
    parent.AddPage(tab, tab.descr)

  def on_list_folders_button(self, event):
    # wx.MessageBox(self.emails.GetValue())
    folderData = ar3_mailrepo_lib.get_folders_for_email(self.credentials_root, self.emails.GetValue())
    populate_grid_with_dict_list(self.grid, folderData)

  def __init__(self, parent, credentials_root: Path):
    wx.Panel.__init__(self, parent)
    self.descr = 'Email Folders'
    self.credentials_root = credentials_root
    emails = util_lib.retrieve_all_email_labels(credentials_root)
    if not emails:
      emails = ['No emails available']
      self.descr = 'No Emails'

    self.emails = wx.ComboBox(self, id=wx.ID_ANY, value=emails[0], pos=wx.DefaultPosition,
                              size=wx.DefaultSize, choices=emails, style=wx.CB_READONLY,
                              validator=wx.DefaultValidator,
                              name=self.descr)

    sizer = wx.BoxSizer(wx.VERTICAL)
    sizer.Add(self.emails, wx.SizerFlags().Border(wx.TOP | wx.LEFT, 25))

    ok_button = wx.Button(self, wx.ID_OK, label='List Folders')
    ok_button.Bind(wx.EVT_BUTTON, self.on_list_folders_button)

    sizer.Add(ok_button, wx.SizerFlags().Border(wx.TOP | wx.LEFT, 25))

    self.grid = wx.grid.Grid(self)
    self.grid.CreateGrid(20, 20)
    self.grid.SetRowLabelSize(1)

    sizer.Add(self.grid, wx.SizerFlags().Border(wx.TOP | wx.LEFT, 25))

    self.SetSizer(sizer)


class EmailListing(wx.Panel):

  @staticmethod
  def add_as_page(parent, credentials_root: Path):
    tab = EmailListing(parent, credentials_root)
    parent.AddPage(tab, tab.descr)

  @staticmethod
  def _pop_password(rec):
    rec.pop('imap_password', None)
    return rec

  def __init__(self, parent, credentials_root: Path):
    wx.Panel.__init__(self, parent)
    self.descr = 'Available Emails'
    self.credentials_root = credentials_root
    emails = util_lib.retrieve_all_email_labels(credentials_root)
    emaildata = [util_lib.load_generic_credentials(credentials_root, x) for x in emails]
    emaildata = list(map(EmailListing._pop_password, emaildata))


    sizer = wx.BoxSizer(wx.VERTICAL)

    self.grid = wx.grid.Grid(self)
    self.grid.CreateGrid(20, 20)
    self.grid.SetRowLabelSize(1)

    sizer.Add(self.grid, wx.SizerFlags().Border(wx.TOP | wx.LEFT, 25))

    self.SetSizer(sizer)

    populate_grid_with_dict_list(self.grid, emaildata)


class MainAppFrame(wx.Frame):
  """
  A Frame that says Hello World
  """

  def _create_emaillist_combobx(self, parent):
    emails = util_lib.retrieve_all_email_labels(
      credential_root_path=self.config.credentials_root())
    return wx.ComboBox(parent, id=wx.ID_ANY, value=emails[0], pos=wx.DefaultPosition,
                       size=wx.DefaultSize, choices=emails, style=0,
                       validator=wx.DefaultValidator,
                       name='Emails with Credentials')

  def __init__(self, *args, **kw):
    self.config = ar3_mailrepo_config.AppConfig.from_configfile(
      'ar3_mailreport_config.yaml')

    # ensure the parent's __init__ is called
    super(MainAppFrame, self).__init__(*args, **kw)

    # create a panel in the frame
    pnl = wx.Panel(self)
    tabcontainer = wx.Notebook(pnl)

    EmailListing.add_as_page(tabcontainer, self.config.credentials_root())
    EmailFolderListingTab.add_as_page(tabcontainer, self.config.credentials_root())

    sizer = wx.BoxSizer(wx.VERTICAL)
    sizer.Add(tabcontainer, 1, wx.EXPAND)
    pnl.SetSizer(sizer)

    self.makeMenuBar()

    self.CreateStatusBar()
    self.SetStatusText(ar3_mailrepo_version_info.into_string())

  def makeMenuBar(self):
    fileMenu = wx.Menu()
    testItem = fileMenu.Append(-1, "&Test...\tCtrl-T",
                                "Test Menu Item")
    fileMenu.AppendSeparator()
    exitItem = fileMenu.Append(wx.ID_EXIT)
    helpMenu = wx.Menu()
    aboutItem = helpMenu.Append(wx.ID_ABOUT)
    menuBar = wx.MenuBar()
    menuBar.Append(fileMenu, "&File")
    menuBar.Append(helpMenu, "&Help")
    self.SetMenuBar(menuBar)
    self.Bind(wx.EVT_MENU, self.OnTestItem, testItem)
    self.Bind(wx.EVT_MENU, self.OnExit, exitItem)
    self.Bind(wx.EVT_MENU, self.OnAbout, aboutItem)

  def OnExit(self, event):
    self.Close(True)

  def OnTestItem(self, event):
    wx.MessageBox('Test')

  def OnAbout(self, event):
    wx.MessageBox(f'{ar3_mailrepo_version_info.into_string()}\n'
                  f'Running on {platform.python_implementation()} '
                  f'V {platform.python_version()} on {platform.platform()}\n',
                  f'About {ar3_mailrepo_version_info.app_name()}',
                  wx.OK | wx.ICON_INFORMATION)


def run_main_gui():
  app = wx.App()
  frm = MainAppFrame(None,
                     title=f'EXPERIMENTAL GUI {ar3_mailrepo_version_info.app_name()}',
                     size=(1250, 750))
  frm.Center()
  frm.Show()
  app.MainLoop()
