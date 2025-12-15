using System;
using System.Windows;
using System.Windows.Forms;
using System.IO;
using System.Reflection;

namespace ZenodoDownloader
{
    public partial class App : System.Windows.Application
    {
        private NotifyIcon? notifyIcon;
        private MainWindow? mainWindow;

        protected override void OnStartup(StartupEventArgs e)
        {
            base.OnStartup(e);

            // 创建托盘图标
            notifyIcon = new NotifyIcon();
            
            // 加载托盘图标 - 优先从资源加载
            System.Drawing.Icon? trayIcon = null;
            string iconSource = "未知";
            
            // 方法1: 尝试从WPF资源加载（pack URI）
            if (trayIcon == null)
            {
                try
                {
                    var iconUri = new Uri("pack://application:,,,/downloader.ico", UriKind.Absolute);
                    var iconStream = System.Windows.Application.GetResourceStream(iconUri);
                    if (iconStream != null)
                    {
                        using (iconStream.Stream)
                        {
                            trayIcon = new System.Drawing.Icon(iconStream.Stream);
                            iconSource = "WPF资源 (pack URI)";
                        }
                    }
                }
                catch (Exception ex)
                {
                    System.Diagnostics.Debug.WriteLine($"从WPF资源加载图标失败: {ex.Message}");
                }
            }
            
            // 方法2: 尝试从嵌入资源加载（Manifest Resource）
            if (trayIcon == null)
            {
                try
                {
                    var assembly = Assembly.GetExecutingAssembly();
                    // 尝试不同的资源名称格式
                    string[] resourceNames = {
                        "ZenodoDownloader.downloader.ico",
                        "downloader.ico",
                        assembly.GetName().Name + ".downloader.ico"
                    };
                    
                    foreach (var resourceName in resourceNames)
                    {
                        using (var stream = assembly.GetManifestResourceStream(resourceName))
                        {
                            if (stream != null)
                            {
                                trayIcon = new System.Drawing.Icon(stream);
                                iconSource = $"嵌入资源 ({resourceName})";
                                break;
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    System.Diagnostics.Debug.WriteLine($"从嵌入资源加载图标失败: {ex.Message}");
                }
            }
            
            // 方法3: 如果资源加载失败，使用系统默认图标
            if (trayIcon == null)
            {
                trayIcon = System.Drawing.SystemIcons.Application;
                iconSource = "系统默认图标";
            }
            
            // 设置托盘图标
            notifyIcon.Icon = trayIcon;
            notifyIcon.Text = "Zenodo Dataset Downloader";
            
            // 创建上下文菜单
            var contextMenu = new ContextMenuStrip();
            
            var showMenuItem = new ToolStripMenuItem("Show Window");
            showMenuItem.Click += ShowMenuItem_Click;
            contextMenu.Items.Add(showMenuItem);

            contextMenu.Items.Add(new ToolStripSeparator());

            var exitMenuItem = new ToolStripMenuItem("Exit");
            exitMenuItem.Click += ExitMenuItem_Click;
            contextMenu.Items.Add(exitMenuItem);

            notifyIcon.ContextMenuStrip = contextMenu;
            notifyIcon.DoubleClick += NotifyIcon_DoubleClick;
            
            // 重要：必须在设置所有属性后再设置Visible为true
            notifyIcon.Visible = true;
            
            System.Diagnostics.Debug.WriteLine($"Tray icon created, icon source: {iconSource}");
            System.Diagnostics.Debug.WriteLine($"Tray icon visibility: {notifyIcon.Visible}");
            System.Diagnostics.Debug.WriteLine($"Tray icon text: {notifyIcon.Text}");
            
            // 显示测试通知（可选，用于确认托盘图标工作）
            try
            {
                notifyIcon.ShowBalloonTip(2000, "Zenodo Downloader", "Program started, icon in system tray", ToolTipIcon.Info);
            }
            catch
            {
                // 忽略通知错误
            }

            // 创建主窗口但不立即显示
            mainWindow = new MainWindow();
            mainWindow.Closing += MainWindow_Closing;
            mainWindow.Show();
        }

        private void ShowMenuItem_Click(object? sender, System.EventArgs e)
        {
            ShowMainWindow();
        }

        private void NotifyIcon_DoubleClick(object? sender, System.EventArgs e)
        {
            ShowMainWindow();
        }

        private void ShowMainWindow()
        {
            if (mainWindow != null)
            {
                mainWindow.Show();
                mainWindow.WindowState = WindowState.Normal;
                mainWindow.Activate();
            }
        }

        private void MainWindow_Closing(object? sender, System.ComponentModel.CancelEventArgs e)
        {
            // 取消关闭，改为隐藏
            e.Cancel = true;
            if (mainWindow != null)
            {
                mainWindow.Hide();
            }
        }

        private void ExitMenuItem_Click(object? sender, System.EventArgs e)
        {
            // 清理资源
            notifyIcon?.Dispose();
            Shutdown();
        }


        protected override void OnExit(ExitEventArgs e)
        {
            // 确保清理托盘图标
            notifyIcon?.Dispose();
            base.OnExit(e);
        }
    }
}

