# Vearch增量备份设计文档

# Vearch增量备份设计文档

# 系统概述

## 设计目标

新版备份恢复系统旨在提供以下能力：


1. **高可用性**：支持异步备份恢复，不阻塞业务操作


1. **可监控性**：提供实时进度查询和任务状态监控


1. **版本管理**：支持多版本备份，便于数据回滚和恢复


1. **容错能力**：支持节点故障检测和自动重试


1. **性能优化**：支持多分区并行备份，提升备份效率

## 核心特性


- ✅**异步执行**：备份恢复任务异步执行，立即返回任务ID


- ✅**多版本**：使用VersionID（时间戳）和BackupID（UUID）双重标识


- ✅**任务监控**：实时监控任务状态，支持进度查询


- ✅**增量备份**：支持文件去重，节省存储空间


- ✅**健康检查**：自动检测节点健康状态，处理节点故障

## 系统架构

### 整体架构图

```mermaid
graph TB
    subgraph "Master 节点"
        API[BackupService<br/>API入口]
        BM[BackupManager<br/>备份管理器]
        VM[VersionManager<br/>版本管理器]
        MON[BackupMonitor<br/>任务监控器]
        
        API --> BM
        BM --> VM
        BM --> MON
        MON --> VM
    end
    
    subgraph "PS 节点"
        RPC[IncrementBackupHandler<br/>RPC处理器]
        PSM[PSShardManager<br/>分片管理器]
        RCM[RefCountManager<br/>引用计数管理器]
        
        RPC --> PSM
        PSM --> RCM
    end
    
    subgraph "S3 对象存储"
        S3[(S3存储)]
        SCHEMA[Schema文件]
        META[分区元信息]
        FILES[数据文件]
        REF[引用计数]
        
        S3 --> SCHEMA
        S3 --> META
        S3 --> FILES
        S3 --> REF
    end
    
    BM -.RPC调用.-> RPC
    MON -.状态查询.-> RPC
    PSM -.上传/下载.-> S3
    RCM -.读写.-> REF
    
    style API fill:#e1f5ff
    style BM fill:#b3e5fc
    style VM fill:#81d4fa
    style MON fill:#4fc3f7
    style RPC fill:#fff9c4
    style PSM fill:#fff59d
    style RCM fill:#ffecb3
    style S3 fill:#c8e6c9

```

#### 2.3.2 组件关系图

```mermaid
classDiagram
    class BackupService {
        +SpaceSnapshot()
        +GetBackupProgress()
    }
    
    class BackupManager {
        +createSnapshot()
        +restoreSnapshot()
        +backupSchema()
        +restoreSchema()
        +resolvePartitions()
    }
    
    class VersionManager {
        +CreateVersion()
        +RestoreVersion()
        +generateVersionID()
        -versionCache
    }
    
    class BackupMonitor {
        +addVersionTask()
        +addRestoreTasks()
        +dispatchTasks()
        +processBackupTask()
        +processRestoreTask()
        +checkVersionStatus()
        +GetBackupProgress()
        -tasks
        -ProcessingVersion
    }
    
    class IncrementBackupHandler {
        +Execute()
    }
    
    class PSShardManager {
        +StartPartitionBackup()
        +StartPartitionRestore()
        +executeShardSnapshot()
        +restoreShardSnapshot()
        -taskStatus
    }
    
    class RefCountManager {
        +GetVersionFiles()
        +IncrementRefCount()
        +DecrementRefCount()
        +QueryFileExists()
        -refCountMap
    }
    
    BackupService --> BackupManager
    BackupManager --> VersionManager
    BackupManager --> BackupMonitor
    BackupMonitor --> IncrementBackupHandler
    IncrementBackupHandler --> PSShardManager
    PSShardManager --> RefCountManager

```

---

## 三、核心组件设计

### 3.1 Master侧组件

#### 3.1.1 BackupService

**职责**：提供备份恢复API入口，协调各个组件完成备份恢复任务。

**主要方法**：


- `SpaceSnapshot()`：处理备份/恢复请求的主入口


- `GetBackupProgress()`：查询备份进度

**设计要点**：


- 统一使用`BackupManager`实例，避免状态丢失


- 自动生成BackupID（UUID）和VersionID（时间戳）



- 支持命令：`backup`、`restore`

---

#### 3.1.2 BackupManager

**职责**：备份恢复的核心调度器，负责协调整个备份恢复流程。

**主要方法**：


- `createSnapshot()`：创建备份快照


- `restoreSnapshot()`：恢复备份快照


- `backupSchema()`：备份Space的Schema信息


- `restoreSchema()`：恢复Space的Schema信息


- `resolvePartitions()`：解析分区信息

**数据结构**：

```plaintext
type BackupManager struct {
    client         *client.Client
    backupMonitor  *BackupMonitor
    versionManager *VersionManager
    // S3分区ID到新分区ID的映射关系
    s3PartitionMap   map[string]map[entity.PartitionID]entity.PartitionID
    muS3PartitionMap sync.RWMutex
}
```

**设计要点**：


- 备份流程：备份Schema → 创建版本 → 分发任务



- 恢复流程：恢复Schema → 建立分区映射 → 分发任务



- 分区映射：恢复时建立S3分区ID到新分区ID的映射关系


---

#### 3.1.3 VersionManager

**职责**：管理备份版本信息，生成版本ID，维护版本缓存。

**主要方法**：


- `CreateVersion()`：创建新版本


- `RestoreVersion()`：创建恢复版本


- `generateVersionID()`：生成版本ID（时间戳格式）

**数据结构**：

```plaintext
type VersionManager struct {
    mu          sync.RWMutex
    client      *client.Client
    minioClient *minio.Client
    bucketName  string
    versionCache map[string]*VersionInfo // key: space_key
    maxVersions int
    stopChan    chanstruct{}
}
type VersionInfo struct {
    SpaceKey    string           // db-space
    Versions    []*BackupVersion // 版本列表
    LastUpdated time.Time
    TotalCount  int64
}
type BackupVersion struct {
    SpaceKey    string
    VersionID   string  // 时间戳格式：202411201504
    BackupID    string  // UUID格式
    CreateTime  time.Time
    Status      BackupVersionStatus
    Partitions  []*PartitionBackupInfo
}
```

**设计要点**：


- VersionID格式：`YYYYMMDDHHmm`（例如：`202411201504`）


- BackupID格式：UUID（例如：`550e8400-e29b-41d4-a716-446655440000`）


- 版本状态：Inited → Running → Completed/Failed



- 版本缓存：内存缓存，提高查询效率


---

#### 3.1.4 BackupMonitor

**职责**：监控备份恢复任务的执行状态，处理节点故障和任务重试。

**主要方法**：


- `addVersionTask()`：添加版本任务


- `addRestoreTasks()`：添加恢复任务（不创建版本）


- `dispatchTasks()`：分发任务到PS节点


- `processBackupTask()`：处理备份任务


- `processRestoreTask()`：处理恢复任务


- `checkVersionStatus()`：检查版本状态


- `GetBackupProgress()`：获取备份进度

**数据结构**：

```plaintext
type BackupMonitor struct {
    mu                sync.RWMutex
    client            *client.Client
    tasks             map[string][]*PartitionBackupTask // key: spaceKey
    ProcessingVersion []*BackupVersion
    healthCheckChan   chan entity.NodeID
    stopChan          chanstruct{}
}
type PartitionBackupTask struct {
    PartitionID   entity.PartitionID
    NodeID        entity.NodeID
    VersionID     string
    PSNodeAddr    string
    TaskType      string  // "backup" or "restore"
    Status        BackupTaskStatus
    RetryCount    int
    MaxRetries    int
    StartTime     time.Time
    CompleteTime  time.Time
    BackupRequest *entity.SnapshotRequest
}
```

**设计要点**：


- 任务状态：Inited → Running → Completed/Failed



- 定期检查：每10秒检查版本状态，每30秒检查任务状态



- 节点健康检查：检测节点故障，等待分区迁移后重试



- 进度查询：基于任务完成数量计算进度


---

### 3.2 PS侧组件

#### 3.2.1 IncrementBackupHandler

**职责**：处理来自Master的备份恢复RPC请求。

**主要方法**：


- `Execute()`：处理RPC请求

**设计要点**：


- 解析`SnapshotRequest`请求


- 获取或初始化`PSShardManager`


- 调用`StartPartitionBackup()`或`StartPartitionRestore()`


- 异步执行，立即返回


---

#### 3.2.2 PSShardManager

**职责**：在PS节点上执行具体的备份恢复操作。

**主要方法**：


- `StartPartitionBackup()`：启动分区备份


- `StartPartitionRestore()`：启动分区恢复


- `executeShardSnapshot()`：执行分片快照（上传文件）


- `restoreShardSnapshot()`：执行分片恢复（下载文件）


- `loadPartitionMeta()`：加载分区元信息

**数据结构**：

```go
type PSShardManager struct {
    mu            sync.RWMutex
    versions      map[string]services.VersionInfo
    taskStatus    map[string]*SnapshotTask // key: "spaceKey_backupID_partitionID"
    minioClient   *minio.Client
    getPartition  GetPartitionFunc
    refCountMgrs  map[string]*RefCountManager // 按spaceKey存储
    bucketName    string
    clusterName   string
    sstStoragePath string
}
type SnapshotTask struct {
    PartitionID   uint32
    s3PartitionID uint32  // S3上的分区ID（恢复时使用）
    status        SnapshotStatus
    spaceKey      string
    backupID      string
    versionID     string
    errorMessage  string
    startTime     time.Time
    completeTime  time.Time
}
```

**设计要点**：


- 备份流程：调用Engine.BackupSpace() → 扫描文件 → 计算CRC32 → 上传到S3



- 恢复流程：下载元信息 → 下载文件 → 验证CRC32 → 恢复Engine



- 分区ID映射：恢复时使用S3分区ID加载数据


---

#### 3.2.3 RefCountManager

**职责**：管理文件的引用计数，实现文件去重。

**主要方法**：


- `GetVersionFiles()`：获取版本文件列表


- `IncrementRefCount()`：增加引用计数


- `DecrementRefCount()`：减少引用计数


- `QueryFileExists()`：查询文件是否存在

**数据结构**：

```go
type RefCountManager struct {
    mu            sync.RWMutex
    refCountMap   map[string]*FileRefCount // key: CRC32
    minioClient   *minio.Client
    bucketName    string
    clusterName   string
    spaceKey      string
    metadataPath  string
}
type FileRefCount struct {
    CRC32         string    // 文件的CRC32值（唯一标识）
    S3Path        string    // 文件在S3上的路径
    RefCount      int       // 引用计数
    Size          int64     // 文件大小
    CreatedAt     time.Time
    LastUpdatedAt time.Time
    Versions      []string  // 引用该文件的版本列表
}
```

**设计要点**：


- 文件去重：相同CRC32的文件只存储一份



- 引用计数：记录文件被多少个版本引用



- 自动清理：引用计数为0时自动删除文件



- 按库表细分：每个spaceKey有独立的引用计数管理器


---

## 四、数据流程设计

### 4.1 备份流程

#### 4.1.1 备份序列图

```mermaid
sequenceDiagram
    participant Client
    participant BackupService
    participant BackupManager
    participant VersionManager
    participant BackupMonitor
    participant PSNode
    participant S3
    
    Client->>BackupService: POST /space/{db}/{space}/backup
    BackupService->>BackupManager: createSnapshot()
    BackupManager->>S3: 连接S3客户端
    BackupManager->>VersionManager: generateVersionID()
    VersionManager-->>BackupManager: VersionID
    BackupManager->>S3: backupSchema() 上传Schema
    BackupManager->>BackupManager: resolvePartitions()
    BackupManager->>VersionManager: CreateVersion()
    VersionManager-->>BackupManager: BackupVersion
    BackupManager->>BackupMonitor: addVersionTask()
    BackupMonitor->>BackupMonitor: dispatchTasks()
    
    loop 每个分区
        BackupMonitor->>PSNode: RPC: BackupSpace1()
        PSNode->>PSNode: Engine.BackupSpace()
        PSNode->>PSNode: 扫描本地文件
        PSNode->>PSNode: 计算CRC32
        PSNode->>S3: 查询文件是否存在
        alt 文件不存在
            PSNode->>S3: 上传文件
            PSNode->>S3: 更新引用计数
        end
        PSNode->>S3: 上传分区元信息
        PSNode-->>BackupMonitor: 任务完成
    end
    
    BackupMonitor->>BackupMonitor: checkVersionStatus()
    BackupMonitor->>VersionManager: 更新版本状态
    BackupService-->>Client: 返回BackupID

```

 


#### 4.1.2 备份流程图

```mermaid
flowchart TD
    Start([开始备份]) --> ConnectS3[连接S3客户端]
    ConnectS3 --> GenID[生成VersionID和BackupID]
    GenID --> BackupSchema[备份Schema到S3]
    BackupSchema --> ResolvePart[解析分区信息]
    ResolvePart --> CreateVersion[创建版本]
    CreateVersion --> AddTasks[添加备份任务]
    AddTasks --> Dispatch{分发任务}
    
    Dispatch --> Task1[分区1任务]
    Dispatch --> Task2[分区2任务]
    Dispatch --> Task3[分区N任务]
    
    Task1 --> Engine1[Engine.BackupSpace]
    Task2 --> Engine2[Engine.BackupSpace]
    Task3 --> EngineN[Engine.BackupSpace]
    
    Engine1 --> Scan1[扫描文件]
    Engine2 --> Scan2[扫描文件]
    EngineN --> ScanN[扫描文件]
    
    Scan1 --> CRC1[计算CRC32]
    Scan2 --> CRC2[计算CRC32]
    ScanN --> CRCN[计算CRC32]
    
    CRC1 --> Check1{文件存在?}
    CRC2 --> Check2{文件存在?}
    CRCN --> CheckN{文件存在?}
    
    Check1 -->|否| Upload1[上传到S3]
    Check2 -->|否| Upload2[上传到S3]
    CheckN -->|否| UploadN[上传到S3]
    
    Check1 -->|是| UpdateRef1[更新引用计数]
    Check2 -->|是| UpdateRef2[更新引用计数]
    CheckN -->|是| UpdateRefN[更新引用计数]
    
    Upload1 --> UpdateRef1
    Upload2 --> UpdateRef2
    UploadN --> UpdateRefN
    
    UpdateRef1 --> Meta1[上传元信息]
    UpdateRef2 --> Meta2[上传元信息]
    UpdateRefN --> MetaN[上传元信息]
    
    Meta1 --> Complete1[任务完成]
    Meta2 --> Complete2[任务完成]
    MetaN --> CompleteN[任务完成]
    
    Complete1 --> CheckAll{所有任务完成?}
    Complete2 --> CheckAll
    Complete3 --> CheckAll
    
    CheckAll -->|否| Wait[等待]
    Wait --> CheckAll
    CheckAll -->|是| End([备份完成])
    
    style Start fill:#c8e6c9
    style End fill:#c8e6c9
    style CheckAll fill:#fff9c4

```

### 4.2 恢复流程

#### 4.2.1 恢复序列图

```mermaid
sequenceDiagram
    participant Client
    participant BackupService
    participant BackupManager
    participant BackupMonitor
    participant PSNode
    participant S3
    
    Client->>BackupService: POST /space/{db}/{space}/backup (restore)
    BackupService->>BackupManager: restoreSnapshot()
    BackupManager->>S3: 连接S3客户端
    BackupManager->>S3: 下载Schema文件
    BackupManager->>BackupManager: 验证分区数量
    BackupManager->>BackupManager: 创建Space
    BackupManager->>BackupManager: 建立分区ID映射
    BackupManager->>BackupManager: resolvePartitions()
    BackupManager->>BackupMonitor: addRestoreTasks()
    BackupMonitor->>BackupMonitor: dispatchTasks()
    
    loop 每个分区
        BackupMonitor->>PSNode: RPC: BackupSpace1()
        PSNode->>PSNode: Engine.Close()
        PSNode->>S3: loadPartitionMeta()
        S3-->>PSNode: 分区元信息
        PSNode->>PSNode: 清空本地目录
        
        loop 每个文件
            PSNode->>S3: 下载文件
            PSNode->>PSNode: 验证CRC32
            PSNode->>PSNode: 保存到本地
        end
        
        PSNode->>PSNode: Engine.Load()
        PSNode-->>BackupMonitor: 任务完成
    end
    
    BackupMonitor->>BackupMonitor: 检查所有任务完成
    BackupService-->>Client: 返回成功

```

 


#### 4.2.2 恢复流程图

```mermaid
flowchart TD
    Start([开始恢复]) --> ConnectS3[连接S3客户端]
    ConnectS3 --> DownloadSchema[下载Schema文件]
    DownloadSchema --> Validate[验证分区数量]
    Validate --> CreateSpace[创建Space]
    CreateSpace --> MapPart[建立分区ID映射]
    MapPart --> ResolvePart[解析新分区信息]
    ResolvePart --> AddTasks[添加恢复任务]
    AddTasks --> Dispatch{分发任务}
    
    Dispatch --> Task1[分区1任务]
    Dispatch --> Task2[分区2任务]
    Dispatch --> TaskN[分区N任务]
    
    Task1 --> Close1[Engine.Close]
    Task2 --> Close2[Engine.Close]
    TaskN --> CloseN[Engine.Close]
    
    Close1 --> LoadMeta1[加载分区元信息]
    Close2 --> LoadMeta2[加载分区元信息]
    CloseN --> LoadMetaN[加载分区元信息]
    
    LoadMeta1 --> Clear1[清空本地目录]
    LoadMeta2 --> Clear2[清空本地目录]
    LoadMetaN --> ClearN[清空本地目录]
    
    Clear1 --> Download1[下载文件]
    Clear2 --> Download2[下载文件]
    ClearN --> DownloadN[下载文件]
    
    Download1 --> Verify1[验证CRC32]
    Download2 --> Verify2[验证CRC32]
    DownloadN --> VerifyN[验证CRC32]
    
    Verify1 --> Save1[保存到本地]
    Verify2 --> Save2[保存到本地]
    VerifyN --> SaveN[保存到本地]
    
    Save1 --> Load1[Engine.Load]
    Save2 --> Load2[Engine.Load]
    SaveN --> LoadN[Engine.Load]
    
    Load1 --> Complete1[任务完成]
    Load2 --> Complete2[任务完成]
    LoadN --> CompleteN[任务完成]
    
    Complete1 --> CheckAll{所有任务完成?}
    Complete2 --> CheckAll
    CompleteN --> CheckAll
    
    CheckAll -->|否| Wait[等待]
    Wait --> CheckAll
    CheckAll -->|是| End([恢复完成])
    
    style Start fill:#c8e6c9
    style End fill:#c8e6c9
    style CheckAll fill:#fff9c4

```

 


### 4.3 任务监控流程

#### 4.3.1 监控流程图

```mermaid
flowchart TD
    Start([BackupMonitor启动]) --> Monitor1[运行任务监控器<br/>每30秒]
    Start --> Monitor2[版本检查器<br/>每10秒]
    
    Monitor1 --> CheckTasks[检查处理中的版本]
    CheckTasks --> LoopTasks{遍历所有任务}
    
    LoopTasks --> QueryStatus[RPC查询PS节点状态]
    QueryStatus --> UpdateStatus[更新任务状态]
    UpdateStatus --> CheckHealth{节点健康?}
    
    CheckHealth -->|否| WaitMigration[等待分区迁移]
    WaitMigration --> Retry[在新节点重试]
    Retry --> LoopTasks
    
    CheckHealth -->|是| CheckTimeout{任务超时?}
    CheckTimeout -->|是| MarkFailed[标记任务失败]
    CheckTimeout -->|否| LoopTasks
    
    MarkFailed --> LoopTasks
    LoopTasks -->|完成| Monitor1
    
    Monitor2 --> CheckVersion[检查版本状态]
    CheckVersion --> AllComplete{所有任务完成?}
    AllComplete -->|是| UpdateVersion[更新版本状态为Completed]
    AllComplete -->|否| Monitor2
    UpdateVersion --> RemoveVersion[从ProcessingVersion移除]
    RemoveVersion --> Monitor2
    
    style Start fill:#c8e6c9
    style CheckHealth fill:#fff9c4
    style AllComplete fill:#fff9c4

```

---

## 五、存储设计

### 5.1 S3路径结构

#### 5.1.1 新版系统路径格式

#### 5.1.2 存储结构树形图

```plaintext
{cluster}/
├── backup/
│   └── {db}/
│       └── {space}/
│           └── {versionID}/
│               ├── {space}.schema                    # Space的Schema文件
│               └── {partitionID}/                    # 分区目录
│                   ├── partition_meta.json            # 分区元信息
│                   └── files/                         # 文件目录（可选）
│                       └── {crc32}.sst                # 数据文件（以CRC32命名）
│
└── metadata/
    └── {db}/
        └── {space}/
            └── file_refcount.json                     # 引用计数元数据
```

### 5.2 文件命名规则


- **Schema文件**：`{spaceName}.schema`


- **分区元信息**：`partition_meta.json`


- **数据文件**：`{crc32}.sst`（使用CRC32值作为文件名，实现去重）

### 5.3 元数据结构

#### 5.3.1 PartitionBackupMeta（分区备份元信息）

```plaintext
{
  "partition_id":1,
  "backup_id":"550e8400-e29b-41d4-a716-446655440000",
  "version_id":"202411201504",
  "space_key":"testdb-testspace",
  "created_at":"2024-11-20T15:04:00Z",
  "files":[
    {
      "file_path":"data/file1.sst",
      "file_name":"file1.sst",
      "crc32":"a1b2c3d4",
      "size":1024000,
      "s3_path":"{cluster}/backup/{db}/{space}/{versionID}/files/a1b2c3d4.sst",
      "local_path":"/path/to/data/file1.sst",
      "uploaded_at":"2024-11-20T15:05:00Z",
      "upload_status":"completed"
    }
  ],
  "total_files":10,
  "total_size":10240000,
  "completed_at":"2024-11-20T15:10:00Z"}
```

#### 5.3.2 FileRefCount（文件引用计数）

```plaintext
{
  "a1b2c3d4":{
    "crc32":"a1b2c3d4",
    "s3_path":"{cluster}/backup/{db}/{space}/{versionID}/files/a1b2c3d4.sst",
    "ref_count":3,
    "size":1024000,
    "created_at":"2024-11-20T15:05:00Z",
    "last_updated_at":"2024-11-20T15:10:00Z",
    "versions":[
      "202411201504",
      "202411201505",
      "202411201506"
    ]
  }}
```

---

## 六、接口设计

### 6.1 API接口

#### 6.1.1 备份接口

**请求**：

```plaintext
POST /space/{db}/{space}/backup
Content-Type: application/json

{
  "command": "backup",
  "backup_id": 0,
  "s3_param": {
    "endpoint": "s3.example.com",
    "access_key": "your-access-key",
    "secret_key": "your-secret-key",
    "bucket_name": "backup-bucket",
    "region": "us-east-1",
    "use_ssl": true
  }
}
```

**响应**：

```plaintext
{
  "code":0,
  "msg":"success",
  "data":{
    "backup_id":0  // 兼容字段，实际使用UUID
  }}
```

#### 6.1.2 恢复接口

**请求**：

```plaintext
POST /space/{db}/{space}/backup
Content-Type: application/json

{
  "command": "restore",
  "version_id": "202411201504",  // 必需（或提供backup_id）
  "s3_param": {
    "endpoint": "s3.example.com",
    "access_key": "your-access-key",
    "secret_key": "your-secret-key",
    "bucket_name": "backup-bucket",
    "region": "us-east-1",
    "use_ssl": true
  }
}
```

**响应**：

```plaintext
{
  "code":0,
  "msg":"success",
  "data":{
    "backup_id":0
  }}
```

#### 6.1.3 进度查询接口

**请求**：

```plaintext
GET /space/{db}/{space}/backup/progress
```

**响应**：

```plaintext
{
  "code":0,
  "msg":"success",
  "data":{
    "total_tasks":3,
    "completed_tasks":2,
    "success_ratio":0.67
  }}
```

### 6.2 RPC接口

#### 6.2.1 备份/恢复RPC

**请求**：`IncrementBackupHandler.Execute()`

**参数**：

```plaintext
type SnapshotRequest struct {
    Database    string
    Space       string
    Command     string  // "backup" or "restore"
    BackupID    string  // UUID
    VersionID   string  // 时间戳格式
    S3PartitionID uint32  // S3分区ID（恢复时使用）
    S3Param     struct {
        Region     string
        BucketName string
        EndPoint   string
        AccessKey  string
        SecretKey  string
        UseSSL     bool
    }
}
```

**响应**：成功返回nil，失败返回错误

#### 6.2.2 状态查询RPC

**请求**：`GetBackupStatus()`

**参数**：


- `spaceKey`: 库表标识


- `backupID`: 备份ID


- `partitionID`: 分区ID

**响应**：

```plaintext
type BackupStatusResponse struct {
    Status      int    // 0=running, 1=completed, 2=failed
    Exists      bool
    ErrorMessage string
}
```

---

## 七、错误处理设计

### 7.1 错误分类

#### 7.1.1 参数错误


- **场景**：无效的VersionID、BackupID、S3参数等


- **处理**：立即返回错误，不创建任务

#### 7.1.2 资源错误


- **场景**：Space不存在、分区不存在、S3连接失败等


- **处理**：返回错误，记录日志

#### 7.1.3 节点故障


- **场景**：PS节点宕机、网络中断等


- **处理**：


   1. 检测节点健康状态



   1. 等待分区迁移到新节点



   1. 在新节点上重试任务



   1. 如果超时，标记任务失败


#### 7.1.4 任务失败


- **场景**：文件上传失败、CRC32校验失败等


- **处理**：


   1. 标记任务为失败



   1. 记录错误信息


       


## 八、性能优化设计

### 8.1 并行处理


- **多分区并行**：所有分区同时备份/恢复


- **文件并行上传**：多个文件并发上传到S3


- **任务并行分发**：使用goroutine并发分发任务

### 8.2 文件去重

```mermaid
graph TB
    subgraph "Master节点"
        BM[BackupManager<br/>备份管理器<br/>━━━━━━━━━━━━━━━━<br/>• 接收备份/恢复请求<br/>• 协调备份任务<br/>• 分发任务到PS节点]
        VM[VersionManager<br/>版本管理器<br/>━━━━━━━━━━━━━━━━<br/>• 生成VersionID<br/>• 管理版本元数据<br/>• 维护版本缓存]
        BM2[BackupMonitor<br/>备份监控器<br/>━━━━━━━━━━━━━━━━<br/>• 监控任务状态<br/>• 处理节点故障<br/>• 定期检查完成情况]
        
        BM -.->|① 创建版本| VM
        VM -.->|② 返回VersionID| BM
        BM -.->|③ 添加任务| BM2
        BM2 -.->|④ 查询状态| VM
        BM -.->|⑤ RPC调用| IBH
        BM2 -.->|⑥ RPC查询状态| IBH
    end
    
    subgraph "PS节点"
        IBH[IncrementBackupHandler<br/>增量备份处理器<br/>━━━━━━━━━━━━━━━━<br/>• 接收Master RPC请求<br/>• 启动备份/恢复任务<br/>• 管理任务状态]
        PSM[PSShardManager<br/>分片管理器<br/>━━━━━━━━━━━━━━━━<br/>• 扫描本地文件<br/>• 计算CRC32<br/>• 上传/下载文件<br/>• 生成分区元数据]
        RCM[RefCountManager<br/>引用计数管理器<br/>━━━━━━━━━━━━━━━━<br/>• 查询文件是否存在<br/>• 管理引用计数<br/>• 自动清理文件]
        
        IBH -->|⑦ 启动任务| PSM
        PSM -->|⑧ 查询文件| RCM
        RCM -->|⑨ 返回信息| PSM
        PSM -->|⑩ 更新计数| RCM
    end
    
    subgraph "对象存储 S3"
        S3[S3对象存储<br/>━━━━━━━━━━━━━━━━]
        S3_1[Schema文件<br/>cluster/backup/.../schema]
        S3_2[分区元数据<br/>.../partition_meta.json]
        S3_3[数据文件<br/>.../data/*.sst]
        S3_4[引用计数元数据<br/>.../file_refcount.json]
        
        S3 --> S3_1
        S3 --> S3_2
        S3 --> S3_3
        S3 --> S3_4
    end
    
    BM ==>|上传/读取| S3_1
    PSM ==>|上传/读取| S3_2
    PSM ==>|上传/读取| S3_3
    RCM ==>|加载/保存| S3_4
    
    style BM fill:#4A90E2,color:#fff,stroke:#2E5C8A,stroke-width:3px
    style VM fill:#4A90E2,color:#fff,stroke:#2E5C8A,stroke-width:3px
    style BM2 fill:#4A90E2,color:#fff,stroke:#2E5C8A,stroke-width:3px
    style IBH fill:#50C878,color:#fff,stroke:#2D7A4E,stroke-width:3px
    style PSM fill:#50C878,color:#fff,stroke:#2D7A4E,stroke-width:3px
    style RCM fill:#50C878,color:#fff,stroke:#2D7A4E,stroke-width:3px
    style S3 fill:#FFD700,color:#000,stroke:#CC9900,stroke-width:3px

```

#### 8.2.1 文件去重流程图

```mermaid
flowchart TD
    Start([备份文件]) --> Scan[扫描本地文件]
    Scan --> CalcCRC[计算CRC32]
    CalcCRC --> Query{查询S3是否存在}
    
    Query -->|存在| IncRef[增加引用计数]
    Query -->|不存在| Upload[上传文件到S3]
    
    Upload --> CreateRef[创建引用计数=1]
    CreateRef --> AddVersion[添加到版本文件列表]
    
    IncRef --> AddVersion
    AddVersion --> Next{还有文件?}
    
    Next -->|是| Scan
    Next -->|否| SaveMeta[保存分区元信息]
    SaveMeta --> End([完成])
    
    style Start fill:#c8e6c9
    style End fill:#c8e6c9
    style Query fill:#fff9c4

```

 


#### 8.2.2 引用计数管理图

```mermaid
graph LR
    subgraph "版本1"
        V1[Version: 202411201504]
        V1F1[File: a1b2c3d4.sst]
        V1F2[File: e5f6g7h8.sst]
        V1 --> V1F1
        V1 --> V1F2
    end
    
    subgraph "版本2"
        V2[Version: 202411201505]
        V2F1[File: a1b2c3d4.sst]
        V2F2[File: i9j0k1l2.sst]
        V2 --> V2F1
        V2 --> V2F2
    end
    
    subgraph "S3存储"
        S3F1[a1b2c3d4.sst<br/>RefCount: 2]
        S3F2[e5f6g7h8.sst<br/>RefCount: 1]
        S3F3[i9j0k1l2.sst<br/>RefCount: 1]
    end
    
    V1F1 -.引用.-> S3F1
    V1F2 -.引用.-> S3F2
    V2F1 -.引用.-> S3F1
    V2F2 -.引用.-> S3F3
    
    style S3F1 fill:#fff9c4
    style S3F2 fill:#c8e6c9
    style S3F3 fill:#c8e6c9

```


- **CRC32校验**：使用CRC32值作为文件唯一标识


- **引用计数**：相同文件只存储一份，多个版本共享


- **存储优化**：节省S3存储空间，减少上传时间

### 8.3 缓存机制


- **版本缓存**：VersionManager维护版本信息缓存


- **任务状态缓存**：BackupMonitor维护任务状态缓存


- **引用计数缓存**：RefCountManager维护引用计数缓存

### 8.4 批量操作


- **批量查询状态**：可以考虑批量查询PS节点状态（未来优化）


- **批量更新引用计数**：批量更新引用计数，减少S3写入次数

---

## 九、兼容性设计


- **统一入口**：`backupSpace`API统一使用新系统


- **参数兼容**：支持旧版参数格式


- **响应兼容**：响应格式保持兼容


## 十、增量备份优势分析

### 10.1 增量备份核心机制

新版备份系统通过**文件去重**和**引用计数**机制实现增量备份，相比传统全量备份具有显著优势。

#### 10.1.1 文件去重机制

```mermaid
graph TB
    subgraph "全量备份模式"
        A1[版本1: 1000个文件<br/>100GB] --> S1[S3存储: 100GB]
        A2[版本2: 1000个文件<br/>100GB] --> S2[S3存储: 100GB]
        A3[版本3: 1000个文件<br/>100GB] --> S3[S3存储: 100GB]
        S1 --> Total1[总存储: 300GB]
        S2 --> Total1
        S3 --> Total1
    end
    
    subgraph "增量备份模式"
        B1[版本1: 1000个文件<br/>100GB] --> T1[S3存储: 100GB]
        B2[版本2: 1000个文件<br/>95%重复] --> T2[S3存储: 5GB新文件<br/>+ 95GB引用]
        B3[版本3: 1000个文件<br/>95%重复] --> T3[S3存储: 5GB新文件<br/>+ 95GB引用]
        T1 --> Total2[总存储: 110GB]
        T2 --> Total2
        T3 --> Total2
    end
    
    style Total1 fill:#ffcdd2
    style Total2 fill:#c8e6c9

```

 


#### 10.1.2 工作原理


1. **CRC32校验**：每个文件计算CRC32值作为唯一标识


1. **引用查询**：备份前查询引用计数管理器，检查文件是否已存在


1. **智能上传**：


  - 文件已存在 → 跳过上传，直接引用，增加引用计数



  - 文件不存在 → 上传新文件，创建引用计数记录



1. **版本共享**：多个版本通过引用计数共享相同文件

### 10.2 存储空间节省分析

#### 10.2.1 典型场景计算

假设场景：


- **初始备份**：1000个文件，总大小100GB


- **数据变化率**：每次备份5%的文件发生变化


- **备份频率**：每天1次


- **保留版本数**：30个版本

**全量备份模式**：

```plaintext
版本1: 100GB
版本2: 100GB
版本3: 100GB
...
版本30: 100GB
总存储 = 100GB × 30 = 3000GB
```

**增量备份模式**：

```plaintext
版本1: 100GB (全量)
版本2: 5GB (新文件) + 95GB (引用) = 100GB逻辑，5GB物理
版本3: 5GB (新文件) + 95GB (引用) = 100GB逻辑，5GB物理
...
版本30: 5GB (新文件) + 95GB (引用) = 100GB逻辑，5GB物理

总存储 = 100GB (版本1) + 5GB × 29 = 245GB
```

**节省比例**：

```plaintext
节省空间 = 3000GB - 245GB = 2755GB
节省比例 = 2755GB / 3000GB = 91.8%
```

#### 10.2.2 不同变化率下的节省效果

现在假设进行一个月的增量备份，第一天的全量数据为 100G，根据不同数据变化率的具体情况如下。


| 数据变化率 | 30个版本总存储（全量） | 30个版本总存储（增量） | 节省空间 | 节省比例 |
|---|---|---|---|---|
| 1% | 3000GB | 129GB | 2871GB | 95.7% |
| 5% | 3000GB | 245GB | 2755GB | 91.8% |
| 10% | 3000GB | 390GB | 2610GB | 87.0% |
| 20% | 3000GB | 680GB | 2320GB | 77.3% |
| 50% | 3000GB | 1550GB | 1450GB | 48.3% |


**结论**：数据变化率越低，增量备份的存储节省效果越明显。对于大多数实际场景（变化率&lt;10%），可以节省80%以上的存储空间。

### 10.3 上传速度提升分析

#### 10.3.1 上传时间对比

假设条件：


- **网络带宽**：100Mbps (12.5MB/s)


- **文件数量**：1000个文件


- **平均文件大小**：100MB


- **数据变化率**：5%

**全量备份模式**：

```plaintext
总数据量 = 1000 × 100MB = 100GB上传时间 = 100GB / 12.5MB/s = 8000秒 ≈ 2.2小时
```

**增量备份模式**：

```plaintext
新文件数量 = 1000 × 5% = 50个文件新文件大小 = 50 × 100MB = 5GB上传时间 = 5GB / 12.5MB/s = 400秒 ≈ 6.7分钟文件去重检查时间 = 1000 × 0.01秒 = 10秒（本地CRC32计算和查询）总时间 = 400秒 + 10秒 = 410秒 ≈ 6.8分钟
```

**速度提升**：

```plaintext
时间节省 = 8000秒 - 410秒 = 7590秒速度提升 = 8000秒 / 410秒 = 19.5倍
```

#### 10.3.2 不同变化率下的速度提升

```mermaid
graph TB
    subgraph "上传时间对比（100GB数据）"
        A1[全量备份<br/>2.2小时] --> A2[增量备份<br/>变化率1%<br/>1.3分钟]
        B1[全量备份<br/>2.2小时] --> B2[增量备份<br/>变化率5%<br/>6.8分钟]
        C1[全量备份<br/>2.2小时] --> C2[增量备份<br/>变化率10%<br/>13.3分钟]
        D1[全量备份<br/>2.2小时] --> D2[增量备份<br/>变化率20%<br/>26.7分钟]
    end
    
    style A2 fill:#c8e6c9
    style B2 fill:#c8e6c9
    style C2 fill:#fff9c4
    style D2 fill:#ffecb3

```


| 数据变化率 | 全量备份时间 | 增量备份时间 | 时间节省 | 速度提升 |
|---|---|---|---|---|
| 1% | 2.2小时 | 1.3分钟 | 2.18小时 | 100倍 |
| 5% | 2.2小时 | 6.8分钟 | 2.13小时 | 19.5倍 |
| 10% | 2.2小时 | 13.3分钟 | 2.07小时 | 10倍 |
| 20% | 2.2小时 | 26.7分钟 | 1.93小时 | 5倍 |


**结论**：增量备份可以显著减少上传时间，特别是在数据变化率较低的场景下，速度提升可达10-100倍。

### 10.4 实际应用场景

#### 10.4.1 场景1：向量数据库日常备份

**场景描述**：


- 数据库规模：10TB数据，1000个分区



- 备份频率：每天1次



- 数据变化率：3%（主要是新增数据，少量更新）


**全量备份**：


- 每次备份：10TB



- 30天存储：10TB × 30 = 300TB



- 每次备份时间：约22小时（假设100Mbps带宽）


**增量备份**：


- 首次备份：10TB



- 后续备份：10TB × 3% = 300GB



- 30天存储：10TB + 300GB × 29 = 18.7TB



- 每次备份时间：约40分钟


**收益**：


- 存储节省：300TB → 18.7TB，节省**93.8%**


- 时间节省：22小时 → 40分钟，速度提升**33倍**

#### 10.4.2 场景2：定期全量+增量混合备份

**场景描述**：


- 每周一次全量备份 + 每天增量备份



- 数据规模：5TB



- 数据变化率：5%


**全量备份策略**：


- 每周全量：5TB × 52周 = 260TB/年



- 存储成本高


**混合备份策略**：


- 每周全量：5TB × 52周 = 260TB



- 每天增量：5TB × 5% × 365天 = 91.25TB



- 总存储：351.25TB


**纯增量备份策略**：


- 首次全量：5TB



- 后续增量：5TB × 5% × 364天 = 91TB



- 总存储：96TB


**收益**：


- 相比全量备份：节省**63%**存储空间


- 相比混合备份：节省**73%**存储空间

### 10.5 技术优势总结

#### 10.5.1 核心优势对比图

```mermaid
graph TB
    subgraph "全量备份"
        A1[每次备份全部数据]
        A2[存储空间线性增长]
        A3[上传时间长]
        A4[网络带宽占用大]
        A5[存储成本高]
    end
    
    subgraph "增量备份"
        B1[只备份变化数据]
        B2[存储空间缓慢增长]
        B3[上传时间短]
        B4[网络带宽占用小]
        B5[存储成本低]
    end
    
    A1 -.对比.-> B1
    A2 -.对比.-> B2
    A3 -.对比.-> B3
    A4 -.对比.-> B4
    A5 -.对比.-> B5
    
    style A1 fill:#ffcdd2
    style A2 fill:#ffcdd2
    style A3 fill:#ffcdd2
    style A4 fill:#ffcdd2
    style A5 fill:#ffcdd2
    
    style B1 fill:#c8e6c9
    style B2 fill:#c8e6c9
    style B3 fill:#c8e6c9
    style B4 fill:#c8e6c9
    style B5 fill:#c8e6c9

```

#### 10.5.2 优势列表


| 优势维度 | 全量备份 | 增量备份 | 提升效果 |
|---|---|---|---|
| **存储空间** | 线性增长 | 缓慢增长 | 节省80-95% |
| **上传速度** | 慢 | 快 | 提升10-100倍 |
| **网络带宽** | 占用大 | 占用小 | 减少80-95% |
| **备份时间** | 长 | 短 | 缩短90%以上 |
| **存储成本** | 高 | 低 | 降低80-95% |
| **恢复灵活性** | 需要完整版本 | 支持任意版本 | 相同 |
| **数据完整性** | 高 | 高 | 相同 |


### 10.6 适用场景建议

#### 10.6.1 最适合的场景

**数据变化率低**（&lt;10%）


- 向量数据库日常备份



- 文档数据库定期备份



- 配置数据备份


**备份频率高**


- 每天多次备份



- 实时备份需求


**存储成本敏感**


- 大规模数据备份



- 长期数据保留


**网络带宽有限**


- 跨地域备份



- 云上备份到云下


#### 10.6.2 注意事项

⚠️**数据变化率高**（&gt;50%）


- 增量备份优势不明显



- 建议使用全量备份或混合策略


⚠️**首次备份**


- 首次备份仍需全量上传



- 后续备份才能享受增量优势


⚠️**版本清理**


- 删除版本时需要更新引用计数



- 引用计数为0的文件会自动清理


---

## 十一、扩展性设计

### 10.1 版本管理扩展


- **版本清理策略**：支持配置最大保留版本数


- **版本过期机制**：支持自动清理过期版本


- **版本标签**：支持为版本添加标签和描述

### 10.2 备份策略扩展


- **增量备份**：支持增量备份（基于引用计数）


- **全量备份**：支持全量备份


- **定时备份**：支持定时自动备份

### 10.3 监控扩展


- **任务详情**：支持查询任务详细信息


- **历史记录**：支持查询历史备份记录


- **告警机制**：支持备份失败告警

### 10.4 性能扩展


- **压缩支持**：支持备份数据压缩


- **加密支持**：支持备份数据加密


- **分片上传**：支持大文件分片上传

---

## 十一、安全性设计

### 11.1 数据安全


- **CRC32校验**：文件上传和下载时进行CRC32校验


- **元数据验证**：恢复时验证元数据完整性


- **Schema验证**：恢复时验证Schema完整性

### 11.2 访问控制


- **S3凭证**：使用用户提供的S3凭证，不存储明文密码


- **权限控制**：备份恢复操作需要相应权限

### 11.3 数据隔离


- **路径隔离**：不同集群、数据库、Space使用独立路径


- **引用计数隔离**：按spaceKey隔离引用计数

---

## 十二、系统特性总结

### 12.1 系统优势

新版备份恢复系统通过以下设计实现了高可用、可监控、高性能的备份恢复能力：


1. **异步架构**：任务异步执行，不阻塞业务


1. **版本管理**：支持多版本备份，便于数据管理


1. **任务监控**：实时监控任务状态，支持进度查询


1. **容错机制**：自动处理节点故障和任务重试


1. **性能优化**：多分区并行、文件去重、缓存机制


1. **向后兼容**：兼容旧系统，平滑升级

### 12.2 技术架构图

```mermaid
graph TB
    subgraph "用户层"
        User[用户/API调用]
    end
    
    subgraph "Master层"
        API[BackupService API]
        BM[BackupManager<br/>调度器]
        VM[VersionManager<br/>版本管理]
        MON[BackupMonitor<br/>任务监控]
    end
    
    subgraph "PS层"
        RPC[RPC Handler]
        PSM[PSShardManager<br/>分片管理]
        RCM[RefCountManager<br/>引用计数]
    end
    
    subgraph "存储层"
        S3[(S3对象存储)]
    end
    
    User --> API
    API --> BM
    BM --> VM
    BM --> MON
    MON --> RPC
    RPC --> PSM
    PSM --> RCM
    PSM --> S3
    RCM --> S3
    
    style User fill:#e1f5ff
    style API fill:#b3e5fc
    style BM fill:#81d4fa
    style VM fill:#4fc3f7
    style MON fill:#29b6f6
    style RPC fill:#fff9c4
    style PSM fill:#fff59d
    style RCM fill:#ffecb3
    style S3 fill:#c8e6c9

```

系统设计充分考虑了扩展性和可维护性，为未来的功能增强奠定了基础。

 


