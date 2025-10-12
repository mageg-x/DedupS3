<template>
  <div class="debug-tool-container">
    <h2>{{ t('debug.metadataViewer') }}</h2>
    
    <!-- 查询区域 -->
    <div class="query-section">
      <el-select v-model="metadataType" class="query-select">
          <el-option :label="t('debug.objectType')" value="object" />
          <el-option :label="t('debug.chunkType')" value="chunk" />
          <el-option :label="t('debug.blockType')" value="block" />
        </el-select>
      <el-input
        v-model="queryId"
        :placeholder="t('debug.queryPlaceholder')"
        class="query-input"
        clearable
        @keyup.enter="queryMetadata"
      />
      <el-button
        type="primary"
        :loading="isLoading"
        @click="queryMetadata"
        class="query-button"
      >
        <search class="query-icon" />
        {{ t('debug.queryButton') }}
      </el-button>
    </div>
    
    <!-- 结果操作区域 -->
    <div v-if="metadataResult" class="result-actions">
      <span class="result-title">{{ t('debug.queryResults') }}</span>
      <div class="action-buttons">
        <el-button
          @click="toggleViewMode"
          class="view-mode-button"
        >
          <document class="action-icon" />
          {{ viewMode === 'formatted' ? t('debug.showAsJson') : t('debug.formattedView') }}
        </el-button>
        <el-button @click="downloadJson" class="action-button">
          <download class="action-icon" />
          {{ t('debug.downloadJson') }}
        </el-button>
        <el-button @click="copyToClipboard" class="action-button">
          <document-copy class="action-icon" />
          {{ t('debug.copyToClipboard') }}
        </el-button>
      </div>
    </div>
    
    <!-- 结果展示区域 -->
    <div v-if="isLoading" class="loading-container">
      <div class="w-12 h-12 border-4 border-blue-200 border-t-blue-600 rounded-full animate-spin mx-auto mb-4"></div>
      <span>{{ t('debug.querying') }}</span>
    </div>
    
    <div v-else-if="metadataResult" class="result-container">
      <!-- 格式化视图 -->
      <div v-if="viewMode === 'formatted'" class="formatted-view">
        <!-- 对象详情 -->
        <div v-if="metadataType === 'object'" class="metadata-card">
          <h3>{{ t('debug.objectMetadata') }}</h3>
          <div class="metadata-grid">
            <div class="metadata-item">
              <label>{{ t('debug.bucket') }}</label>
              <span>{{ metadataResult.bucket }}</span>
            </div>
            <div class="metadata-item">
              <label>{{ t('debug.key') }}</label>
              <span>{{ metadataResult.key }}</span>
            </div>
            <div class="metadata-item">
              <label>{{ t('debug.etag') }}</label>
              <span>{{ metadataResult.etag }}</span>
            </div>
            <div class="metadata-item">
              <label>{{ t('debug.size') }}</label>
              <span>{{ formatData(metadataResult.size) }}</span>
            </div>
            <div class="metadata-item">
              <label>{{ t('debug.chunksCount') }}</label>
              <span>{{ metadataResult.chunks?.length || 0 }}</span>
            </div>
            <div class="metadata-item">
              <label>{{ t('debug.createdAt') }}</label>
              <span>{{ formatData(metadataResult.createdAt, 'createdAt') }}</span>
            </div>
            <div class="metadata-item">
              <label>{{ t('debug.lastModified') }}</label>
              <span>{{ formatData(metadataResult.lastModified, 'lastModified') }}</span>
            </div>
            <div class="metadata-item">
              <label>{{ t('debug.dataLocation') }}</label>
              <span>{{ metadataResult.dataLocation }}</span>
            </div>
          </div>
          
          <!-- 切片列表 -->
          <div v-if="metadataResult.chunks && metadataResult.chunks.length > 0" class="list-section">
            <h4>{{ t('debug.chunkList') }}</h4>
            <el-table :data="metadataResult.chunks.map((chunk, index) => ({ index, id: chunk }))" size="small">
              <el-table-column prop="index" label="序号" width="80" />
              <el-table-column prop="id" label="切片ID" min-width="200" show-overflow-tooltip />
            </el-table>
          </div>
        </div>
        
        <!-- 切片详情 -->
        <div v-else-if="metadataType === 'chunk'" class="metadata-card">
          <h3>{{ t('debug.chunkMetadata') }}</h3>
          <div class="metadata-grid">
            <div class="metadata-item">
              <label>{{ t('debug.hash') }}</label>
              <span>{{ metadataResult.hash }}</span>
            </div>
            <div class="metadata-item">
              <label>{{ t('debug.size') }}</label>
              <span>{{ formatData(metadataResult.size) }}</span>
            </div>
            <div class="metadata-item">
              <label>{{ t('debug.ref_count') }}</label>
              <span>{{ metadataResult.ref_count }}</span>
            </div>
            <div class="metadata-item">
              <label>{{ t('debug.block_id') }}</label>
              <span>{{ metadataResult.block_id }}</span>
            </div>
          </div>
        </div>
        
        <!-- 块详情 -->
        <div v-else-if="metadataType === 'block'" class="metadata-card">
          <h3>{{ t('debug.blockMetadata') }}</h3>
          <div class="metadata-grid">
            <div class="metadata-item">
              <label>{{ t('debug.id') }}</label>
              <span>{{ metadataResult.id }}</span>
            </div>
            <div class="metadata-item">
              <label>{{ t('debug.ver') }}</label>
              <span>{{ metadataResult.ver }}</span>
            </div>
            <div class="metadata-item">
              <label>{{ t('debug.etag') }}</label>
              <span>{{ formatData(metadataResult.etag, 'etag') }}</span>
            </div>
            <div class="metadata-item">
              <label>{{ t('debug.total_size') }}</label>
              <span>{{ formatData(metadataResult.total_size) }}</span>
            </div>
            <div class="metadata-item">
              <label>{{ t('debug.real_size') }}</label>
              <span>{{ formatData(metadataResult.real_size) }}</span>
            </div>
            <div class="metadata-item">
              <label>{{ t('debug.compressed') }}</label>
              <span>{{ formatData(metadataResult.compressed) }}</span>
            </div>
            <div class="metadata-item">
              <label>{{ t('debug.encrypted') }}</label>
              <span>{{ formatData(metadataResult.encrypted) }}</span>
            </div>
            <div class="metadata-item">
              <label>{{ t('debug.location') }}</label>
              <span>{{ metadataResult.location }}</span>
            </div>
            <div class="metadata-item">
              <label>{{ t('debug.storage_id') }}</label>
              <span>{{ metadataResult.storage_id }}</span>
            </div>
            <div class="metadata-item">
              <label>{{ t('debug.finally') }}</label>
              <span>{{ formatData(metadataResult.finally) }}</span>
            </div>
            <div class="metadata-item">
              <label>{{ t('debug.created_at') }}</label>
              <span>{{ formatData(metadataResult.created_at, 'created_at') }}</span>
            </div>
            <div class="metadata-item">
              <label>{{ t('debug.updated_at') }}</label>
              <span>{{ formatData(metadataResult.updated_at, 'updated_at') }}</span>
            </div>
          </div>
          
          <!-- 内部切片列表 -->
          <div v-if="metadataResult.chunk_list && metadataResult.chunk_list.length > 0" class="list-section">
            <h4>{{ t('debug.chunkList') }}</h4>
            <el-table :data="metadataResult.chunk_list" size="small">
              <el-table-column prop="hash" :label="t('debug.chunkHash')" min-width="200" show-overflow-tooltip />
              <el-table-column prop="size" :label="t('debug.size')" width="120">
                <template #default="{ row }">
                  {{ formatData(row.size) }}
                </template>
              </el-table-column>
            </el-table>
          </div>
        </div>
      </div>
      
      <!-- JSON视图 -->
      <div v-else class="json-view">
        <pre>{{ JSON.stringify(metadataResult, null, 2) }}</pre>
      </div>
    </div>
    
    <div v-else-if="!isLoading" class="no-data">
      <span>{{ t('debug.noData') }}</span>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref } from 'vue';
import { Search, Download, DocumentCopy, Document } from '@element-plus/icons-vue';
import { ElMessage } from 'element-plus';
import { useI18n } from 'vue-i18n';
import { objectinfo, blockinfo, chunkinfo } from '@/api/admin';

const { t } = useI18n();

// 状态变量
const metadataType = ref('object'); // 'object', 'chunk', 'block'
const queryId = ref('');
const metadataResult = ref<any>(null);
const isLoading = ref(false);
const viewMode = ref<'formatted' | 'json'>('formatted');

// 格式化显示数据
const formatData = (data: any, fieldName?: string) => {
  if (data === null || data === undefined) return '-';
  if (typeof data === 'boolean') return data ? 'true' : 'false';
  if (typeof data === 'object') {
    if (Array.isArray(data)) {
      // 特殊处理etag数组，转换为16进制字符串
      if (fieldName === 'etag') {
        return data.map((byte: number) => {
          const hex = byte.toString(16);
          return hex.length === 1 ? '0' + hex : hex;
        }).join('');
      }
      return data.length === 0 ? '[]' : `[${data.length} 项]`;
    }
    return 'Object';
  }
  // 处理时间格式
  if ((fieldName && (fieldName.includes('Time') || fieldName.includes('At'))) && typeof data === 'string') {
    try {
      const date = new Date(data);
      if (!isNaN(date.getTime())) {
        return date.toLocaleString();
      }
    } catch {
      // 如果转换失败，返回原始数据
    }
  }
  // 处理数字格式，特别是大数字
  if (typeof data === 'number' && data > 1000) {
    if (data > 1000000) {
      return (data / 1000000).toFixed(2) + ' MB';
    }
    return (data / 1000).toFixed(2) + ' KB';
  }
  return String(data);
};

// 生成模拟数据
const generateMockData = () => {
  const currentTime = new Date().toISOString();
  
  switch (metadataType.value) {
    case 'object':
      return {
        Bucket: 'test-bucket',
        Key: 'sample/object/file.txt',
        ETag: 'e10adc3949ba59abbe56e057f20f883e',
        Size: 1024000, // 约1MB
        Chunks: ['chunk-1', 'chunk-2', 'chunk-3'],
        LastModified: currentTime,
        CreatedAt: currentTime,
        DataLocation: 'storage-engine-1',
        ObjType: 1
      };
    case 'chunk':
      return {
        Hash: 'sha256:abcdef1234567890',
        Size: 349525,
        ref_count: 5,
        block_id: 'block-789'
      };
    case 'block':
      return {
        ID: 'block-789',
        Ver: 1,
        Etag: new Array(16).fill(0),
        TotalSize: 1048576,
        RealSize: 838860,
        Compressed: true,
        Encrypted: false,
        Location: 'storage-node-1',
        ChunkList: [
          { Hash: 'chunk-1', Size: 349525 },
          { Hash: 'chunk-2', Size: 349525 },
          { Hash: 'chunk-3', Size: 349525 }
        ],
        Finally: true,
        StorageID: 'storage-1',
        CreatedAt: currentTime,
        UpdatedAt: currentTime
      };
    default:
      return null;
  }
};

// 查询元数据
const queryMetadata = async () => {
  if (!queryId.value.trim()) {
    ElMessage.warning(t('debug.queryPlaceholder'));
    return;
  }
  
  isLoading.value = true;
  metadataResult.value = null;
  
  try {
    let result;
    switch (metadataType.value) {
      case 'object':
        result = await objectinfo({ objectID: queryId.value });
        break;
      case 'chunk':
        result = await chunkinfo({ chunkID: queryId.value });
        break;
      case 'block':
        result = await blockinfo({ blockID: queryId.value });
        break;
      default:
        throw new Error(t('debug.invalidType'));
    }
    
    if (result && result.code === 0) {
      // 成功响应
      metadataResult.value = result.data;
    } else {
      // 错误响应
      ElMessage.error(result?.msg || t('debug.queryFailed'));
    }
  } catch (error) {
    console.error('Query metadata error:', error);
    ElMessage.error(t('debug.queryFailed'));
  } finally {
    isLoading.value = false;
  }
};

// 切换视图模式
const toggleViewMode = () => {
  viewMode.value = viewMode.value === 'formatted' ? 'json' : 'formatted';
};

// 下载JSON数据
const downloadJson = () => {
  if (!metadataResult.value) return;
  
  const dataStr = JSON.stringify(metadataResult.value, null, 2);
  const dataUri = 'data:application/json;charset=utf-8,'+ encodeURIComponent(dataStr);
  
  const exportFileDefaultName = `${metadataType.value}-${queryId.value || 'mock'}-metadata.json`;
  
  const linkElement = document.createElement('a');
  linkElement.setAttribute('href', dataUri);
  linkElement.setAttribute('download', exportFileDefaultName);
  linkElement.click();
  
  ElMessage.success(t('debug.copied'));
};

// 复制到剪贴板
const copyToClipboard = () => {
  if (!metadataResult.value) return;
  
  const dataStr = JSON.stringify(metadataResult.value, null, 2);
  navigator.clipboard.writeText(dataStr).then(() => {
    ElMessage.success(t('debug.copied'));
  }).catch(() => {
      ElMessage.error(t('debug.copyFailed'));
    });
};
</script>

<style scoped>
.debug-tool-container {
  padding: 20px;
  max-width: 1200px;
  margin: 0 auto;
}

.query-section {
  display: flex;
  gap: 16px;
  margin-bottom: 24px;
  align-items: flex-end;
}

.query-select {
  width: 120px;
}

.query-input {
  flex: 1;
  min-width: 300px;
}

.query-button {
  white-space: nowrap;
}

.query-icon {
  margin-right: 4px;
}

.result-actions {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 16px;
}

.result-title {
  font-size: 18px;
  font-weight: 500;
}

.action-buttons {
  display: flex;
  gap: 8px;
}

.action-button,
.view-mode-button {
  display: flex;
  align-items: center;
}

.action-icon {
  margin-right: 4px;
  font-size: 14px;
}

.loading-container {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  height: 300px;
}

.result-container {
  background: #fff;
  border-radius: 8px;
  padding: 20px;
  box-shadow: 0 2px 12px 0 rgba(0, 0, 0, 0.1);
}

.metadata-card {
  background: #fafafa;
  border-radius: 8px;
  padding: 20px;
}

.metadata-card h3 {
  margin-top: 0;
  margin-bottom: 20px;
  color: #303133;
  border-bottom: 1px solid #ebeef5;
  padding-bottom: 10px;
}

.metadata-card h4 {
  margin-top: 20px;
  margin-bottom: 12px;
  color: #606266;
  font-size: 14px;
}

.metadata-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
  gap: 16px;
}

.metadata-item {
  display: flex;
  flex-direction: column;
  padding: 8px;
  border-radius: 4px;
  background: #fff;
  box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
}

.metadata-item label {
  font-size: 12px;
  color: #909399;
  margin-bottom: 4px;
}

.metadata-item span {
  font-size: 14px;
  color: #303133;
  word-break: break-all;
}

.list-section {
  margin-top: 24px;
  background: #fff;
  border-radius: 4px;
  padding: 16px;
  box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
}

.formatted-view {
  display: flex;
  flex-direction: column;
  gap: 16px;
}

.json-view {
  background: #f5f5f5;
  border-radius: 4px;
  padding: 16px;
  max-height: 600px;
  overflow: auto;
}

.json-view pre {
  margin: 0;
  white-space: pre-wrap;
  word-break: break-all;
}

.no-data {
  display: flex;
  align-items: center;
  justify-content: center;
  height: 300px;
  background: #fff;
  border-radius: 8px;
  color: #909399;
}

/* 响应式设计 */
@media (max-width: 768px) {
  .debug-tool-container {
    padding: 12px;
  }
  
  .query-section {
    flex-direction: column;
    gap: 12px;
  }
  
  .query-select,
  .query-input {
    width: 100%;
  }
  
  .metadata-grid {
    grid-template-columns: 1fr;
  }
  
  .result-actions {
    flex-direction: column;
    align-items: flex-start;
    gap: 12px;
  }
  
  .action-buttons {
    width: 100%;
    justify-content: space-between;
  }
  
  .action-button,
  .view-mode-button {
    flex: 1;
    justify-content: center;
  }
}
</style>