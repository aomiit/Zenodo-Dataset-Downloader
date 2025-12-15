using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace ZenodoDownloader
{
    public class DownloadProgress
    {
        public string FileName { get; set; } = "";
        public long BytesDownloaded { get; set; }
        public long TotalBytes { get; set; }
        public double Progress => TotalBytes > 0 ? Math.Min(100, (double)BytesDownloaded / TotalBytes * 100) : 0;
        public string Status { get; set; } = "";
    }

    public class OverallProgress
    {
        public int FilesCompleted { get; set; }
        public int TotalFiles { get; set; }
        public long TotalBytesDownloaded { get; set; }
        public long TotalBytes { get; set; }
        public double AverageFileProgress { get; set; }
        public double Progress => Math.Min(100, AverageFileProgress);
    }

    public class ChunkProgress
    {
        public int ChunkIndex { get; set; }
        public long BytesDownloaded { get; set; }
        public long TotalBytes { get; set; }
        public double Progress => TotalBytes > 0 ? Math.Min(100, (double)BytesDownloaded / TotalBytes * 100) : 0;
        public string ChunkInfo => $"Chunk {ChunkIndex + 1}: {FormatFileSize(BytesDownloaded)} / {FormatFileSize(TotalBytes)} ({Progress:F1}%)";
        
        private static string FormatFileSize(long bytes)
        {
            string[] sizes = { "B", "KB", "MB", "GB", "TB" };
            double len = bytes;
            int order = 0;
            while (len >= 1024 && order < sizes.Length - 1)
            {
                order++;
                len = len / 1024;
            }
            return $"{len:0.##} {sizes[order]}";
        }
    }

    public class DownloadService
    {
        private readonly HttpClient httpClient = new HttpClient();
        private readonly object lockObject = new object();
        
        // Chunk download configuration
        private const long ChunkSizeThreshold = 10 * 1024 * 1024; // Use chunk download for files >10MB
        private const int MaxChunks = 8; // Maximum number of chunks
        
        // Retry configuration
        private const int MaxRetries = 5; // Maximum retry count
        private const int RetryDelayMs = 2000; // Retry delay (milliseconds)

        private CancellationTokenSource? cancellationTokenSource;
        private PauseTokenSource? pauseTokenSource;
        private Dictionary<int, ChunkProgress> chunkProgresses = new Dictionary<int, ChunkProgress>();
        private Dictionary<string, double> fileProgresses = new Dictionary<string, double>(); // 跟踪每个文件的进度百分比
        private Dictionary<string, long> fileSizes = new Dictionary<string, long>(); // 跟踪每个文件的大小，用于加权平均
        private int totalFiles = 0;
        private long totalSize = 0;
        private int completedFiles = 0;
        private long totalBytesDownloaded = 0;
        private bool isPaused = false;

        public event Action<DownloadProgress>? FileProgressChanged;
        public event Action<OverallProgress>? OverallProgressChanged;
        public event Action<Dictionary<int, ChunkProgress>>? ChunkProgressChanged;
        public event Action<string>? StatusChanged;

        public DownloadService()
        {
            httpClient.DefaultRequestHeaders.Add("User-Agent", "ZenodoDownloader/1.0");
            pauseTokenSource = new PauseTokenSource();
        }

        public void Pause()
        {
            if (!isPaused && pauseTokenSource != null)
            {
                isPaused = true;
                pauseTokenSource.Pause();
                OnStatusChanged("Download paused");
            }
        }

        public void Resume()
        {
            if (isPaused && pauseTokenSource != null)
            {
                isPaused = false;
                pauseTokenSource.Resume();
                OnStatusChanged("Download resumed");
            }
        }

        public bool IsPaused => isPaused;

        public async Task DownloadDatasetAsync(string doi, string outputDir, CancellationToken cancellationToken)
        {
            cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            pauseTokenSource = new PauseTokenSource();
            isPaused = false;
            
            try
            {
                OnStatusChanged($"Parsing DOI: {doi}");

                // Extract record ID from DOI
                if (!doi.Contains("zenodo."))
                {
                    throw new ArgumentException($"Invalid DOI format: {doi}");
                }

                string recordId = doi.Split(new[] { "zenodo." }, StringSplitOptions.None)[1].TrimEnd('/');
                OnStatusChanged($"Record ID: {recordId}");

                // Get record metadata
                OnStatusChanged("Fetching dataset information...");
                string apiUrl = $"https://zenodo.org/api/records/{recordId}";
                var response = await httpClient.GetAsync(apiUrl, cancellationTokenSource.Token);
                response.EnsureSuccessStatusCode();

                var jsonString = await response.Content.ReadAsStringAsync(cancellationTokenSource.Token);
                using var jsonDoc = JsonDocument.Parse(jsonString);
                var root = jsonDoc.RootElement;

                // Check for errors
                if (root.TryGetProperty("message", out var message))
                {
                    throw new Exception($"API error: {message.GetString()}");
                }

                // Get dataset title
                string title = root.TryGetProperty("metadata", out var metadata) &&
                              metadata.TryGetProperty("title", out var titleElement)
                    ? titleElement.GetString() ?? "Unknown Dataset"
                    : "Unknown Dataset";

                OnStatusChanged($"Dataset Title: {title}");

                // Get file list
                if (!root.TryGetProperty("files", out var filesArray) || filesArray.ValueKind != JsonValueKind.Array)
                {
                    throw new Exception("File list not found");
                }

                var fileList = new List<ZenodoFileInfo>();
                foreach (var file in filesArray.EnumerateArray())
                {
                    if (file.TryGetProperty("key", out var key) &&
                        file.TryGetProperty("links", out var links) &&
                        links.TryGetProperty("self", out var self) &&
                        file.TryGetProperty("size", out var size))
                    {
                        fileList.Add(new ZenodoFileInfo
                        {
                            Name = key.GetString() ?? "",
                            Url = self.GetString() ?? "",
                            Size = size.GetInt64()
                        });
                    }
                }

                if (fileList.Count == 0)
                {
                    throw new Exception("No downloadable files found");
                }

                totalSize = fileList.Sum(f => f.Size);
                totalFiles = fileList.Count;
                completedFiles = 0;
                totalBytesDownloaded = 0;
                
                // Initialize file progress tracking
                fileProgresses.Clear();
                fileSizes.Clear();
                foreach (var file in fileList)
                {
                    fileProgresses[file.Name] = 0.0;
                    fileSizes[file.Name] = file.Size;
                }
                
                OnStatusChanged($"Found {fileList.Count} files, total size: {FormatFileSize(totalSize)}");

                // Create output directory
                Directory.CreateDirectory(outputDir);

                // Initialize overall progress
                OnOverallProgressChanged(new OverallProgress
                {
                    FilesCompleted = 0,
                    TotalFiles = totalFiles,
                    TotalBytesDownloaded = 0,
                    TotalBytes = totalSize,
                    AverageFileProgress = 0.0
                });

                // Download files
                OnStatusChanged("Starting download...");
                var startTime = DateTime.Now;

                var semaphore = new SemaphoreSlim(Environment.ProcessorCount * 2, Environment.ProcessorCount * 2);
                var downloadTasks = fileList.Select(async file =>
                {
                    await DownloadFileAsync(file, outputDir, semaphore, cancellationTokenSource.Token);
                    lock (lockObject)
                    {
                        completedFiles++;
                        UpdateOverallProgress();
                    }
                });
                await Task.WhenAll(downloadTasks);

                var elapsed = DateTime.Now - startTime;
                OnStatusChanged($"Download completed! Time elapsed: {elapsed.TotalSeconds:F2} seconds");
            }
            catch (OperationCanceledException)
            {
                OnStatusChanged("Download cancelled");
                throw;
            }
            catch (Exception ex)
            {
                OnStatusChanged($"Error: {ex.Message}");
                throw;
            }
        }

        private async Task DownloadFileAsync(ZenodoFileInfo fileInfo, string outputDir, SemaphoreSlim semaphore, CancellationToken cancellationToken)
        {
            await semaphore.WaitAsync(cancellationToken);
            try
            {
                string filePath = Path.Combine(outputDir, fileInfo.Name);
                
                // Check if file is already complete
                if (File.Exists(filePath))
                {
                    var existingFile = new FileInfo(filePath);
                    if (existingFile.Length == fileInfo.Size)
                    {
                        lock (lockObject)
                        {
                            // Ensure totalBytesDownloaded doesn't exceed totalSize
                            long maxAllowed = totalSize - totalBytesDownloaded;
                            if (maxAllowed > 0)
                            {
                                totalBytesDownloaded += Math.Min(fileInfo.Size, maxAllowed);
                            }
                        }
                        OnFileProgressChanged(new DownloadProgress
                        {
                            FileName = fileInfo.Name,
                            BytesDownloaded = fileInfo.Size,
                            TotalBytes = fileInfo.Size,
                            Status = "Complete, skipped"
                        });
                        return;
                    }
                }

                // Use chunked multi-threaded download for large files
                if (fileInfo.Size >= ChunkSizeThreshold)
                {
                    await DownloadFileWithChunksAsync(fileInfo, filePath, cancellationToken);
                }
                else
                {
                    // Use single-threaded download for small files
                    await DownloadFileSingleThreadAsync(fileInfo, filePath, cancellationToken);
                }

                var finalFile = new FileInfo(filePath);
                if (finalFile.Length == fileInfo.Size)
                {
                    OnFileProgressChanged(new DownloadProgress
                    {
                        FileName = fileInfo.Name,
                        BytesDownloaded = fileInfo.Size,
                        TotalBytes = fileInfo.Size,
                        Status = "Completed"
                    });
                }
            }
            catch (Exception ex)
            {
                OnFileProgressChanged(new DownloadProgress
                {
                    FileName = fileInfo.Name,
                    Status = $"Error: {ex.Message}"
                });
                throw;
            }
            finally
            {
                semaphore.Release();
            }
        }

        private async Task DownloadFileSingleThreadAsync(ZenodoFileInfo fileInfo, string filePath, CancellationToken cancellationToken)
        {
            int retryCount = 0;
            
            while (retryCount <= MaxRetries)
            {
                cancellationToken.ThrowIfCancellationRequested();
                
                try
                {
                    long existingSize = 0;
                    bool isResume = false;
                    
                    // Check if file already exists
                    if (File.Exists(filePath))
                    {
                        var existingFile = new FileInfo(filePath);
                        existingSize = existingFile.Length;
                        
                        if (existingSize > 0 && existingSize < fileInfo.Size)
                        {
                            isResume = true;
                        }
                    }

                    HttpResponseMessage? response = null;

                    // Try resume download
                    if (isResume && existingSize > 0)
                    {
                        using var request = new HttpRequestMessage(HttpMethod.Get, fileInfo.Url);
                        request.Headers.Range = new System.Net.Http.Headers.RangeHeaderValue(existingSize, null);
                        
                        response = await httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
                        
                        if (response.StatusCode == System.Net.HttpStatusCode.PartialContent)
                        {
                            await DownloadStreamAsync(response, filePath, existingSize, fileInfo.Size, true, fileInfo.Name, cancellationToken);
                            response.Dispose();
                            return; // Successfully completed
                        }
                        else
                        {
                            response.Dispose();
                            File.Delete(filePath);
                            existingSize = 0;
                        }
                    }

                    // Download from the beginning
                    using var newRequest = new HttpRequestMessage(HttpMethod.Get, fileInfo.Url);
                    response = await httpClient.SendAsync(newRequest, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
                    response.EnsureSuccessStatusCode();
                    await DownloadStreamAsync(response, filePath, existingSize, fileInfo.Size, false, fileInfo.Name, cancellationToken);
                    response.Dispose();
                    
                    // Verify file size
                    var finalFile = new FileInfo(filePath);
                    if (finalFile.Length != fileInfo.Size)
                    {
                        throw new Exception($"File size mismatch: expected {fileInfo.Size}, actual {finalFile.Length}");
                    }
                    
                    return; // Successfully completed
                }
                catch (Exception ex) when (retryCount < MaxRetries && IsRetryableException(ex) && !cancellationToken.IsCancellationRequested)
                {
                    retryCount++;
                    OnStatusChanged($"[Retry] {fileInfo.Name} - Retry attempt {retryCount}");
                    
                    // Wait before retrying
                    await Task.Delay(RetryDelayMs, cancellationToken);
                    
                    // Check file status
                    if (File.Exists(filePath))
                    {
                        var existingFile = new FileInfo(filePath);
                        if (existingFile.Length == 0 || existingFile.Length > fileInfo.Size)
                        {
                            try { File.Delete(filePath); } catch { }
                        }
                    }
                }
            }
            
            // All retries failed
            throw new Exception($"Download failed after {MaxRetries} retries");
        }

        private async Task DownloadFileWithChunksAsync(ZenodoFileInfo fileInfo, string filePath, CancellationToken cancellationToken)
        {
            // If file exists but is incomplete, delete it (chunk download will recreate it)
            if (File.Exists(filePath))
            {
                var existingFile = new FileInfo(filePath);
                if (existingFile.Length != fileInfo.Size)
                {
                    File.Delete(filePath);
                }
            }

            // Calculate chunk count
            int chunkCount = CalculateChunkCount(fileInfo.Size);
            long chunkSize = fileInfo.Size / chunkCount;
            
            OnStatusChanged($"[Chunk Download] {fileInfo.Name} - {chunkCount} chunks");

            string tempDir = Path.Combine(Path.GetDirectoryName(filePath)!, $".{Path.GetFileName(filePath)}.chunks");
            
            // If temp directory exists (from previous interruption), clean it up
            if (Directory.Exists(tempDir))
            {
                try
                {
                    Directory.Delete(tempDir, true);
                }
                catch
                {
                    // If deletion fails, try to continue using it (some chunks may already be downloaded)
                }
            }
            
            Directory.CreateDirectory(tempDir);

            try
            {
                var chunks = new List<ChunkInfo>();
                chunkProgresses.Clear();
                
                for (int i = 0; i < chunkCount; i++)
                {
                    long start = i * chunkSize;
                    long end = (i == chunkCount - 1) ? fileInfo.Size - 1 : (i + 1) * chunkSize - 1;
                    string chunkFile = Path.Combine(tempDir, $"chunk_{i}.tmp");
                    
                    chunks.Add(new ChunkInfo
                    {
                        Index = i,
                        Start = start,
                        End = end,
                        FilePath = chunkFile
                    });
                    
                    chunkProgresses[i] = new ChunkProgress
                    {
                        ChunkIndex = i,
                        BytesDownloaded = 0,
                        TotalBytes = end - start + 1
                    };
                }

                OnChunkProgressChanged(chunkProgresses);
                
                // Initialize file progress
                OnFileProgressChanged(new DownloadProgress
                {
                    FileName = fileInfo.Name,
                    BytesDownloaded = 0,
                    TotalBytes = fileInfo.Size,
                    Status = "Downloading chunks"
                });

                // Download all chunks concurrently, automatically retry failed chunks until all succeed
                var chunkSemaphore = new SemaphoreSlim(MaxChunks, MaxChunks);
                
                // Continuously retry failed chunks until all chunks succeed
                var failedChunks = new HashSet<int>();
                int retryRound = 0;
                bool isFirstRound = true;
                
                while (true)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    
                    // Check for pause
                    if (pauseTokenSource != null)
                    {
                        await pauseTokenSource.WaitWhilePausedAsync(cancellationToken);
                    }
                    
                    if (!isFirstRound)
                    {
                        if (failedChunks.Count == 0)
                        {
                            // All chunks have succeeded
                            break;
                        }
                        OnStatusChanged($"[Retry] {fileInfo.Name} - Round {retryRound} retry, {failedChunks.Count} chunks remaining");
                        await Task.Delay(RetryDelayMs, cancellationToken);
                    }
                    
                    // Download chunks that need to be downloaded (first round downloads all, subsequent rounds only download failed ones)
                    var chunksToDownload = isFirstRound 
                        ? chunks.ToList()
                        : chunks.Where(chunk => failedChunks.Contains(chunk.Index)).ToList();
                    
                    if (chunksToDownload.Count == 0)
                    {
                        break;
                    }
                    
                    // Mark first round as completed
                    isFirstRound = false;
                    
                    var chunkTasks = chunksToDownload.Select(async chunk =>
                    {
                        try
                        {
                            await DownloadChunkAsync(fileInfo.Url, chunk, chunkSemaphore, fileInfo.Name, fileInfo.Size, cancellationToken);
                            // Verify chunk is complete
                            if (File.Exists(chunk.FilePath))
                            {
                                var chunkFile = new FileInfo(chunk.FilePath);
                                long expectedSize = chunk.End - chunk.Start + 1;
                                if (chunkFile.Length == expectedSize)
                                {
                                    lock (lockObject)
                                    {
                                        failedChunks.Remove(chunk.Index);
                                    }
                                }
                                else
                                {
                                    // Chunk size mismatch, delete and retry
                                    try { File.Delete(chunk.FilePath); } catch { }
                                    lock (lockObject)
                                    {
                                        failedChunks.Add(chunk.Index);
                                    }
                                }
                            }
                            else
                            {
                                // Chunk file doesn't exist, mark as failed
                                lock (lockObject)
                                {
                                    failedChunks.Add(chunk.Index);
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            // Chunk download failed, keep in failed list for retry
                            OnStatusChanged($"[Chunk {chunk.Index + 1} failed] {ex.Message}");
                            lock (lockObject)
                            {
                                failedChunks.Add(chunk.Index);
                            }
                        }
                    });
                    
                    await Task.WhenAll(chunkTasks);
                    retryRound++;
                    
                    // If all chunks succeeded, exit loop
                    if (failedChunks.Count == 0)
                    {
                        break;
                    }
                    
                    // If exceeded maximum retry rounds, throw exception
                    if (retryRound > MaxRetries * 3)
                    {
                        throw new Exception($"{fileInfo.Name} download failed: {failedChunks.Count} chunks still failed after {retryRound} retry rounds");
                    }
                    
                    // Continue retrying failed chunks
                    continue;
                }
                
                // Final verification that all chunks have been successfully downloaded
                foreach (var chunk in chunks)
                {
                    if (!File.Exists(chunk.FilePath))
                    {
                        throw new Exception($"Chunk {chunk.Index + 1} file does not exist");
                    }
                    var chunkFile = new FileInfo(chunk.FilePath);
                    long expectedSize = chunk.End - chunk.Start + 1;
                    if (chunkFile.Length != expectedSize)
                    {
                        throw new Exception($"Chunk {chunk.Index + 1} size mismatch: expected {expectedSize}, actual {chunkFile.Length}");
                    }
                }

                // Merge all chunks into final file
                OnStatusChanged($"[Merging] {fileInfo.Name} - Merging {chunkCount} chunks...");
                OnFileProgressChanged(new DownloadProgress
                {
                    FileName = fileInfo.Name,
                    BytesDownloaded = fileInfo.Size,
                    TotalBytes = fileInfo.Size,
                    Status = "Merging chunks"
                });
                await MergeChunksAsync(chunks, filePath, fileInfo.Size);
                
                // Update file progress to completed
                OnFileProgressChanged(new DownloadProgress
                {
                    FileName = fileInfo.Name,
                    BytesDownloaded = fileInfo.Size,
                    TotalBytes = fileInfo.Size,
                    Status = "Completed"
                });

                // Clean up temporary files
                Directory.Delete(tempDir, true);
            }
            catch
            {
                // Clean up temporary files on error
                if (Directory.Exists(tempDir))
                {
                    try { Directory.Delete(tempDir, true); } catch { }
                }
                throw;
            }
        }

        private int CalculateChunkCount(long fileSize)
        {
            int chunks = (int)Math.Min(MaxChunks, Math.Max(2, fileSize / (20 * 1024 * 1024)));
            return chunks;
        }

        private async Task DownloadChunkAsync(string url, ChunkInfo chunk, SemaphoreSlim semaphore, string fileName, long fileSize, CancellationToken cancellationToken)
        {
            await semaphore.WaitAsync(cancellationToken);
            try
            {
                int retryCount = 0;
                long expectedSize = chunk.End - chunk.Start + 1;
                
                while (retryCount <= MaxRetries)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    
                    try
                    {
                        long existingSize = 0;
                        bool isResume = false;

                        // Check if chunk file already exists
                        if (File.Exists(chunk.FilePath))
                        {
                            var existingFile = new FileInfo(chunk.FilePath);
                            existingSize = existingFile.Length;

                            if (existingSize == expectedSize)
                            {
                                // Chunk is complete
                                lock (lockObject)
                                {
                                    // Ensure expectedSize matches TotalBytes in chunkProgresses
                                    long chunkTotalBytes = chunkProgresses[chunk.Index].TotalBytes;
                                    if (expectedSize != chunkTotalBytes)
                                    {
                                        // Update TotalBytes to match expectedSize if they differ
                                        chunkProgresses[chunk.Index].TotalBytes = expectedSize;
                                        chunkTotalBytes = expectedSize;
                                    }
                                    
                                    long previousBytesDownloaded = chunkProgresses[chunk.Index].BytesDownloaded;
                                    // Ensure BytesDownloaded doesn't exceed TotalBytes
                                    chunkProgresses[chunk.Index].BytesDownloaded = chunkTotalBytes;
                                    
                                    // Only add the difference to avoid double counting
                                    long bytesToAdd = chunkTotalBytes - previousBytesDownloaded;
                                    if (bytesToAdd > 0)
                                    {
                                        // Ensure totalBytesDownloaded doesn't exceed totalSize
                                        long maxAllowed = totalSize - totalBytesDownloaded;
                                        if (maxAllowed > 0)
                                        {
                                            totalBytesDownloaded += Math.Min(bytesToAdd, maxAllowed);
                                        }
                                    }
                                    
                                    OnChunkProgressChanged(chunkProgresses);
                                    
                                    // Calculate total file progress (sum of all downloaded chunks)
                                    long fileBytesDownloaded = chunkProgresses.Values.Sum(cp => cp.BytesDownloaded);
                                    // Ensure fileBytesDownloaded doesn't exceed fileSize
                                    fileBytesDownloaded = Math.Min(fileBytesDownloaded, fileSize);
                                    OnFileProgressChanged(new DownloadProgress
                                    {
                                        FileName = fileName,
                                        BytesDownloaded = fileBytesDownloaded,
                                        TotalBytes = fileSize,
                                        Status = "Downloading chunks"
                                    });
                                }
                                return; // Successfully completed
                            }
                            else if (existingSize > 0 && existingSize < expectedSize)
                            {
                                isResume = true;
                            }
                        }

                        // Download chunk
                        using var request = new HttpRequestMessage(HttpMethod.Get, url);
                        long rangeStart = chunk.Start + existingSize;
                        long rangeEnd = chunk.End;

                        if (isResume)
                        {
                            request.Headers.Range = new System.Net.Http.Headers.RangeHeaderValue(rangeStart, rangeEnd);
                        }
                        else
                        {
                            request.Headers.Range = new System.Net.Http.Headers.RangeHeaderValue(chunk.Start, chunk.End);
                        }

                        using var response = await httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
                        
                        if (response.StatusCode != System.Net.HttpStatusCode.PartialContent && !isResume)
                        {
                            response.Dispose();
                            throw new Exception($"Server does not support Range requests, cannot use chunk download");
                        }

                        response.EnsureSuccessStatusCode();

                        FileMode fileMode = isResume ? FileMode.Append : FileMode.Create;
                        using var fileStream = new FileStream(chunk.FilePath, fileMode, FileAccess.Write, FileShare.None, 8192, true);
                        using var httpStream = await response.Content.ReadAsStreamAsync(cancellationToken);

                        var buffer = new byte[8192];
                        int read;
                        long bytesRead = existingSize;

                        while ((read = await httpStream.ReadAsync(buffer, 0, buffer.Length, cancellationToken)) > 0)
                        {
                            // Check for pause
                            if (pauseTokenSource != null)
                            {
                                await pauseTokenSource.WaitWhilePausedAsync(cancellationToken);
                            }
                            
                            await fileStream.WriteAsync(buffer, 0, read, cancellationToken);
                            bytesRead += read;

                            lock (lockObject)
                            {
                                // Ensure expectedSize matches TotalBytes in chunkProgresses
                                long chunkTotalBytes = chunkProgresses[chunk.Index].TotalBytes;
                                if (expectedSize != chunkTotalBytes)
                                {
                                    // Update TotalBytes to match expectedSize if they differ
                                    chunkProgresses[chunk.Index].TotalBytes = expectedSize;
                                    chunkTotalBytes = expectedSize;
                                }
                                
                                long previousBytes = chunkProgresses[chunk.Index].BytesDownloaded;
                                // Ensure BytesDownloaded doesn't exceed TotalBytes
                                long actualBytesDownloaded = Math.Min(bytesRead, chunkTotalBytes);
                                chunkProgresses[chunk.Index].BytesDownloaded = actualBytesDownloaded;
                                
                                // Only add the difference to avoid double counting, especially on resume
                                long bytesToAdd = actualBytesDownloaded - previousBytes;
                                if (bytesToAdd > 0)
                                {
                                    // Ensure totalBytesDownloaded doesn't exceed totalSize
                                    long maxAllowed = totalSize - totalBytesDownloaded;
                                    if (maxAllowed > 0)
                                    {
                                        totalBytesDownloaded += Math.Min(bytesToAdd, maxAllowed);
                                    }
                                }
                                
                                OnChunkProgressChanged(chunkProgresses);
                                
                                // Calculate total file progress (sum of all downloaded chunks)
                                long fileBytesDownloaded = chunkProgresses.Values.Sum(cp => cp.BytesDownloaded);
                                // Ensure fileBytesDownloaded doesn't exceed fileSize
                                fileBytesDownloaded = Math.Min(fileBytesDownloaded, fileSize);
                                OnFileProgressChanged(new DownloadProgress
                                {
                                    FileName = fileName,
                                    BytesDownloaded = fileBytesDownloaded,
                                    TotalBytes = fileSize,
                                    Status = "Downloading chunks"
                                });
                            }
                        }
                        
                        // Verify chunk size
                        var finalChunkFile = new FileInfo(chunk.FilePath);
                        if (finalChunkFile.Length != expectedSize)
                        {
                            throw new Exception($"Chunk size mismatch: expected {expectedSize}, actual {finalChunkFile.Length}");
                        }
                        
                        return; // Successfully completed
                    }
                    catch (Exception ex) when (retryCount < MaxRetries && IsRetryableException(ex) && !cancellationToken.IsCancellationRequested)
                    {
                        retryCount++;
                        
                        // Check chunk file status
                        if (File.Exists(chunk.FilePath))
                        {
                            var existingFile = new FileInfo(chunk.FilePath);
                            if (existingFile.Length == 0 || existingFile.Length > expectedSize)
                            {
                                try { File.Delete(chunk.FilePath); } catch { }
                            }
                        }
                        
                        await Task.Delay(RetryDelayMs, cancellationToken);
                    }
                }
                
                throw new Exception($"Chunk download failed after {MaxRetries} retries");
            }
            finally
            {
                semaphore.Release();
            }
        }

        private async Task MergeChunksAsync(List<ChunkInfo> chunks, string outputPath, long expectedSize)
        {
            using var outputStream = new FileStream(outputPath, FileMode.Create, FileAccess.Write, FileShare.None, 8192, true);
            
            foreach (var chunk in chunks.OrderBy(c => c.Index))
            {
                if (!File.Exists(chunk.FilePath))
                {
                    throw new Exception($"Chunk file does not exist: {chunk.FilePath}");
                }

                using var chunkStream = new FileStream(chunk.FilePath, FileMode.Open, FileAccess.Read, FileShare.Read, 8192, true);
                await chunkStream.CopyToAsync(outputStream);
            }

            await outputStream.FlushAsync();
            
            // Verify file size
            var finalFile = new FileInfo(outputPath);
            if (finalFile.Length != expectedSize)
            {
                throw new Exception($"File size mismatch: expected {expectedSize}, actual {finalFile.Length}");
            }
        }

        private async Task DownloadStreamAsync(HttpResponseMessage response, string filePath, long existingSize, long totalSize, bool isResume, string fileName, CancellationToken cancellationToken)
        {
            FileMode fileMode = isResume ? FileMode.Append : FileMode.Create;
            
            using var fileStream = new FileStream(filePath, fileMode, FileAccess.Write, FileShare.None, 8192, true);
            using var httpStream = await response.Content.ReadAsStreamAsync(cancellationToken);

            var buffer = new byte[8192];
            long bytesRead = 0;
            int read;
            long totalBytesRead = existingSize;

            try
            {
                        while ((read = await httpStream.ReadAsync(buffer, 0, buffer.Length, cancellationToken)) > 0)
                        {
                            // Check for pause
                            if (pauseTokenSource != null)
                            {
                                await pauseTokenSource.WaitWhilePausedAsync(cancellationToken);
                            }
                            
                            await fileStream.WriteAsync(buffer, 0, read, cancellationToken);
                            bytesRead += read;
                            totalBytesRead += read;

                            // Update file progress
                            // Ensure BytesDownloaded doesn't exceed TotalBytes
                            long actualBytesDownloaded = Math.Min(totalBytesRead, totalSize);
                            
                            lock (lockObject)
                            {
                                // Ensure totalBytesDownloaded doesn't exceed totalSize
                                long maxAllowed = totalSize - totalBytesDownloaded;
                                if (maxAllowed > 0)
                                {
                                    totalBytesDownloaded += Math.Min(read, maxAllowed);
                                }
                            }
                            
                            OnFileProgressChanged(new DownloadProgress
                            {
                                FileName = fileName,
                                BytesDownloaded = actualBytesDownloaded,
                                TotalBytes = totalSize,
                                Status = "Downloading"
                            });
                        }
                
                // If read completed but file size doesn't match, might be EOF error
                if (totalBytesRead < totalSize)
                {
                    throw new IOException($"Download incomplete: downloaded {totalBytesRead} bytes, expected {totalSize} bytes");
                }
            }
            catch (Exception ex) when (IsNetworkException(ex))
            {
                await fileStream.FlushAsync(cancellationToken);
                throw;
            }
        }

        private bool IsRetryableException(Exception ex)
        {
            return IsNetworkException(ex) || 
                   ex is IOException || 
                   ex is System.Net.Http.HttpRequestException ||
                   ex is TaskCanceledException ||
                   (ex.Message.Contains("EOF") || ex.Message.Contains("transport stream"));
        }

        private bool IsNetworkException(Exception ex)
        {
            return ex is System.Net.Http.HttpRequestException ||
                   ex is System.Net.Sockets.SocketException ||
                   ex is System.Net.WebException ||
                   ex is TaskCanceledException ||
                   (ex is IOException && (ex.Message.Contains("EOF") || 
                                          ex.Message.Contains("transport stream") ||
                                          ex.Message.Contains("connection") ||
                                          ex.Message.Contains("network")));
        }

        public static string FormatFileSize(long bytes)
        {
            string[] sizes = { "B", "KB", "MB", "GB", "TB" };
            double len = bytes;
            int order = 0;
            while (len >= 1024 && order < sizes.Length - 1)
            {
                order++;
                len = len / 1024;
            }
            return $"{len:0.##} {sizes[order]}";
        }

        private void OnFileProgressChanged(DownloadProgress progress)
        {
            // Update file progress tracking (only if TotalBytes > 0 to avoid invalid progress)
            lock (lockObject)
            {
                if (progress.TotalBytes > 0)
                {
                    fileProgresses[progress.FileName] = progress.Progress;
                    UpdateOverallProgress();
                }
            }
            
            FileProgressChanged?.Invoke(progress);
        }

        private void UpdateOverallProgress()
        {
            // Ensure totalBytesDownloaded doesn't exceed totalSize
            // This prevents progress from exceeding 100%
            long actualTotalBytesDownloaded = Math.Min(totalBytesDownloaded, totalSize);
            
            // Calculate progress based on actual bytes downloaded
            // This ensures progress is monotonic (always increasing) and accurate
            // Using bytes-based progress instead of file average prevents jumps when files complete
            double averageProgress = totalSize > 0 
                ? (double)actualTotalBytesDownloaded / totalSize * 100.0 
                : 0.0;
            
            // Also calculate weighted average for reference
            // This helps maintain accuracy when files are retrying
            if (fileProgresses.Count > 0 && totalSize > 0)
            {
                // Calculate weighted average: sum(fileProgress * fileSize) / totalSize
                double weightedSum = 0.0;
                foreach (var kvp in fileProgresses)
                {
                    string fileName = kvp.Key;
                    double fileProgress = kvp.Value;
                    
                    // Get file size, default to 0 if not found
                    long fileSize = fileSizes.TryGetValue(fileName, out long size) ? size : 0;
                    
                    // Ensure fileProgress doesn't exceed 100% to prevent weighted average from exceeding 100%
                    double clampedProgress = Math.Min(fileProgress, 100.0);
                    weightedSum += clampedProgress * fileSize;
                }
                
                double weightedAverage = weightedSum / totalSize;
                
                // Use the maximum to ensure progress never decreases, but cap at 100%
                // This prevents progress from jumping backwards when files are retried
                averageProgress = Math.Min(Math.Max(averageProgress, weightedAverage), 100.0);
            }
            
            // Ensure progress never exceeds 100%
            averageProgress = Math.Min(averageProgress, 100.0);
            
            // Update overall progress with average file progress
            OnOverallProgressChanged(new OverallProgress
            {
                FilesCompleted = completedFiles,
                TotalFiles = totalFiles,
                TotalBytesDownloaded = actualTotalBytesDownloaded,
                TotalBytes = totalSize,
                AverageFileProgress = averageProgress
            });
        }

        private void OnOverallProgressChanged(OverallProgress progress)
        {
            OverallProgressChanged?.Invoke(progress);
        }

        private void OnChunkProgressChanged(Dictionary<int, ChunkProgress> progresses)
        {
            ChunkProgressChanged?.Invoke(progresses);
        }

        private void OnStatusChanged(string status)
        {
            StatusChanged?.Invoke(status);
        }

        public void Cancel()
        {
            cancellationTokenSource?.Cancel();
        }
    }

    // PauseTokenSource implementation for pause/resume functionality
    public class PauseTokenSource
    {
        private readonly object _lock = new object();
        private TaskCompletionSource<bool>? _paused;
        private bool _isPaused;

        public bool IsPaused
        {
            get
            {
                lock (_lock)
                {
                    return _isPaused;
                }
            }
        }

        public void Pause()
        {
            lock (_lock)
            {
                if (!_isPaused)
                {
                    _isPaused = true;
                    _paused = new TaskCompletionSource<bool>();
                }
            }
        }

        public void Resume()
        {
            lock (_lock)
            {
                if (_isPaused)
                {
                    _isPaused = false;
                    _paused?.SetResult(true);
                    _paused = null;
                }
            }
        }

        public async Task WaitWhilePausedAsync(CancellationToken cancellationToken = default)
        {
            TaskCompletionSource<bool>? toWait = null;
            lock (_lock)
            {
                if (_isPaused)
                {
                    toWait = _paused ?? new TaskCompletionSource<bool>();
                    _paused = toWait;
                }
            }

            if (toWait != null)
            {
                using (cancellationToken.Register(() => toWait.TrySetCanceled()))
                {
                    await toWait.Task;
                }
            }
        }
    }

    class ZenodoFileInfo
    {
        public string Name { get; set; } = "";
        public string Url { get; set; } = "";
        public long Size { get; set; }
    }

    class ChunkInfo
    {
        public int Index { get; set; }
        public long Start { get; set; }
        public long End { get; set; }
        public string FilePath { get; set; } = "";
    }
}

