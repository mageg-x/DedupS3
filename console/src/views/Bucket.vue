<template>
  <div class="buckets-container">
    <!-- 头部操作区域 -->
    <div class="header-actions">
      <div class="page-title">
        <h1 class="text-xl font-bold text-gray-800 sm:text-2xl m-0">{{ t('bucket.pageTitle') }}</h1>
        <p class="text-gray-500 text-xs sm:text-sm m-0">{{ t('bucket.pageSubtitle') }}</p>
      </div>

      <div class="search-create-container">
        <!-- 搜索框 -->
        <div class="relative flex-grow w-full sm:w-auto min-w-[300px]">
          <input 
            v-model="searchQuery" 
            type="text" 
            :placeholder="t('bucket.searchPlaceholder')"
            class="w-full pl-9 pr-3 py-2.25 rounded-lg border border-gray-200 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent text-sm">
          <i class="fas fa-search absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 text-sm"></i>
        </div>

        <!-- 创建桶按钮 -->
        <button
          class="create-bucket-btn bg-blue-600 hover:bg-blue-700 text-white px-4 py-2.25 rounded-lg font-medium flex items-center justify-center transition-all duration-300 transform hover:scale-105 hover:shadow-md whitespace-nowrap text-sm"
          @click="openCreateBucketDialog">
          <i class="fas fa-plus-circle mr-1.5 text-sm"></i>
          {{ t('bucket.createBucket') }}
        </button>
      </div>
    </div>

    <!-- 桶列表区域 -->
    <div class="buckets-list-container">
      <div class="buckets-list">
        <!-- 加载状态 -->
        <div v-if="loading" class="loading-state py-16 text-center bg-white rounded-xl shadow-md">
          <div class="w-12 h-12 border-4 border-blue-200 border-t-blue-600 rounded-full animate-spin mx-auto mb-4"></div>
          <h3 class="text-xl font-semibold text-gray-800 mb-2">{{ t('bucket.loadingBuckets') }}</h3>
          <p class="text-gray-500">{{ t('bucket.pleaseWait') }}</p>
        </div>

        <!-- 错误状态 -->
        <div v-else-if="error" class="error-state py-16 text-center bg-white rounded-xl shadow-md">
          <div class="w-16 h-16 bg-red-50 rounded-full flex items-center justify-center mx-auto mb-4">
            <i class="fas fa-exclamation-circle text-3xl text-red-500"></i>
          </div>
          <h3 class="text-xl font-semibold text-gray-800 mb-2">{{ t('bucket.error') }}</h3>
          <p class="text-gray-500 mb-6 max-w-md mx-auto">{{ error }}</p>
          <button
            class="retry-btn bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded-lg font-medium flex items-center justify-center transition-all duration-300 mx-auto"
            @click="fetchBuckets">
            <i class="fas fa-sync-alt mr-2"></i>
            {{ t('bucket.retry') }}
          </button>
        </div>

        <!-- 桶列表 -->
        <div v-else-if="paginatedBuckets.length > 0" class="space-y-4">
          <div v-for="(bucket, index) in paginatedBuckets" :key="bucket.name"
            class="bucket-item bg-white rounded-xl shadow-md border border-gray-100 overflow-hidden hover:shadow-lg transition-all duration-300 animate-fadeIn"
            :style="{ animationDelay: index * 0.05 + 's' }">
            <!-- 桶标题和操作按钮 -->
            <div class="bucketTitle p-4 flex items-center justify-between">
              <h1 class="text-2xl font-bold text-gray-800 truncate flex-grow max-w-[calc(100%-100px)]">
                {{ bucket.name }}
              </h1>

              <!-- 操作按钮 - 固定在右侧 -->
              <div class="actions flex space-x-2 flex-shrink-0">
                <button
                  class="action-btn p-2 text-blue-500 hover:text-blue-700 hover:bg-blue-100 rounded-lg transition-all duration-300"
                  :title="t('bucket.viewDetails')">
                  <i class="fas fa-info-circle"></i>
                </button>
                <button
                  class="action-btn p-2 text-green-500 hover:text-green-700 hover:bg-green-100 rounded-lg transition-all duration-300"
                  :title="t('bucket.enterBucket')" @click="navigateToBucket(bucket.name)">
                  <i class="fas fa-folder-open"></i>
                </button>
                <button
                  class="action-btn p-2 text-red-500 hover:text-red-700 hover:bg-red-100 rounded-lg transition-all duration-300"
                  :title="t('bucket.deleteBucket')"
                  @click="handleDeleteBucket(bucket)">
                  <i class="fas fa-trash-alt"></i>
                </button>
              </div>
            </div>

            <!-- 桶详情 -->
            <div class="bucketDetails px-4 pb-3">
              <span class="mr-6">
                <strong>{{ t('bucket.createdAt') }}:</strong> {{ formatDate(bucket.createdAt) }}
              </span>
            </div>

            <!-- 桶指标 -->
            <div class="bucketMetrics px-4 pb-4 flex items-center">
              <a href="#" class="flex-shrink-0 mr-6" @click.prevent="navigateToBucket(bucket.name)">
                <svg xmlns="http://www.w3.org/2000/svg" class="bucketIcon" fill="currentcolor" viewBox="0 0 256 256"
                  style="height: 48px; width: 48px;">
                  <g>
                    <path
                      d="M244.1,8.4c-3.9-5.3-10.1-8.5-16.7-8.5H21.6C15,0,8.8,3.1,4.9,8.4C0.8,14-0.9,21,0.3,27.9c5.1,29.6,15.8,91.9,24.3,141.7v0.1C29,195,32.8,217.1,35,229.9c1.4,10.8,10.4,18.9,21.3,19.3h136.5c10.9-0.4,19.9-8.5,21.3-19.3l10.3-60.1l0.1-0.4L238.4,88v-0.2l10.3-59.9C249.9,21,248.3,14,244.1,8.4 M206.1,177h-163l-3.2-18.6h169.3L206.1,177z M220,95.3H28.9l-3.2-18.6h197.4L220,95.3z" />
                  </g>
                </svg>
              </a>

              <div class="flex items-center gap-8">
                <!-- 使用量指标 -->
                <div class="metric">
                  <svg xmlns="http://www.w3.org/2000/svg" class="min-icon" fill="currentcolor" viewBox="0 0 256 256">
                    <defs>
                      <clipPath id="prefix__a">
                        <path d="M0 0h256v256H0z" />
                      </clipPath>
                    </defs>
                    <g data-name="Reported Usage" clip-path="url(#prefix__a)">
                      <path fill="none" d="M0 0h256v256H0z" />
                      <path data-name="Trazado 390"
                        d="M128.003 0a128.151 128.151 0 0 0-128 128c0 70.573 57.424 127.995 128 127.995a128.147 128.147 0 0 0 128-127.995 128.15 128.15 0 0 0-128-128Zm0 223.078a95.188 95.188 0 0 1-95.085-95.075 95.191 95.191 0 0 1 95.085-95.084v95.084h95.075a95.184 95.184 0 0 1-95.075 95.074Z" />
                      <path data-name="Rect\xE1ngulo 869" fill="none" d="M0 0h256v256H0z" />
                    </g>
                  </svg>
                  <span class="metricLabel">{{ t('bucket.dataSize') }}</span>
                  <div class="metricText">{{ formatSize(bucket.size) }}</div>
                </div>

                <!-- 对象数量指标 -->
                <div class="metric">
                  <svg xmlns="http://www.w3.org/2000/svg" class="min-icon" fill="currentcolor" viewBox="0 0 256 256">
                    <defs>
                      <clipPath id="prefix__a">
                        <path d="M0 0h256v256H0z" />
                      </clipPath>
                    </defs>
                    <g data-name="Total Objects" clip-path="url(#prefix__a)">
                      <path fill="none" d="M0 0h256v256H0z" />
                      <path data-name="total-objects-icn"
                        d="M-.004 128.002a128.148 128.148 0 0 1 128-128 128.148 128.148 0 0 1 128 128 128.144 128.144 0 0 1-128 128 128.144 128.144 0 0 1-128-128Zm19.844 0a108.275 108.275 0 0 0 108.156 108.155 108.28 108.28 0 0 0 108.16-108.155 108.283 108.283 0 0 0-108.16-108.157A108.278 108.278 0 0 0 19.842 128.002Zm27.555 31.581a37.6 37.6 0 0 1 37.564-37.565 37.608 37.608 0 0 1 37.561 37.565 37.609 37.609 0 0 1-37.561 37.565 37.606 37.606 0 0 1-37.563-37.566Zm108.127 34.939a17.425 17.425 0 0 1-17.408-17.4v-37.7a17.429 17.429 0 0 1 17.408-17.407h37.689a17.429 17.429 0 0 1 17.408 17.407v37.7a17.425 17.425 0 0 1-17.408 17.4Zm-54.881-81.311a13.3 13.3 0 0 1-11.477-6.625 13.3 13.3 0 0 1 0-13.249l26.861-46.521a13.287 13.287 0 0 1 11.477-6.629 13.281 13.281 0 0 1 11.475 6.629l26.861 46.521a13.285 13.285 0 0 1 0 13.249 13.294 13.294 0 0 1-11.479 6.625Z"
                        stroke="rgba(0,0,0,0)" stroke-miterlimit="10" />
                      <path data-name="Rect\xE1ngulo 853" fill="none" d="M0 0h256v256H0z" />
                    </g>
                  </svg>
                  <span class="metricLabel">{{ t('bucket.objectCount') }}</span>
                  <div class="metricText">{{ formatNumber(bucket.objectCount) }}</div>
                </div>
              </div>
            </div>
          </div>
        </div>

        <!-- 空状态 -->
        <div v-else class="empty-state py-16 text-center bg-white rounded-xl shadow-md">
          <div class="w-24 h-24 bg-gray-100 rounded-full flex items-center justify-center mx-auto mb-4">
            <svg class="w-12 h-12 text-gray-400" viewBox="0 0 256 256" fill="currentColor"
              xmlns="http://www.w3.org/2000/svg">
              <g>
                <path
                  d="M244.1,8.4c-3.9-5.3-10.1-8.5-16.7-8.5H21.6C15,0,8.8,3.1,4.9,8.4C0.8,14-0.9,21,0.3,27.9c5.1,29.6,15.8,91.9,24.3,141.7v0.1C29,195,32.8,217.1,35,229.9c1.4,10.8,10.4,18.9,21.3,19.3h136.5c10.9-0.4,19.9-8.5,21.3-19.3l10.3-60.1l0.1-0.4L238.4,88v-0.2l10.3-59.9C249.9,21,248.3,14,244.1,8.4 M206.1,177h-163l-3.2-18.6h169.3L206.1,177z M220,95.3H28.9l-3.2-18.6h197.4L220,95.3z" />
              </g>
            </svg>
          </div>
          <h3 class="text-xl font-semibold text-gray-800 mb-2">{{ t('bucket.noBuckets') }}</h3>
          <p class="text-gray-500 mb-6 max-w-md mx-auto">{{ t('bucket.noBucketsHint') }}</p>
        </div>
      </div>

      <!-- 分页 -->
      <div v-if="filteredBuckets.length > 0" class="pagination flex items-center justify-end mt-8">
        <div class="pagination-controls flex items-center gap-2">
          <button
            class="pagination-btn px-3 py-1.5 rounded border border-gray-200 text-gray-500 hover:border-blue-500 hover:text-blue-500 transition-all duration-300"
            :disabled="currentPage === 1" @click="currentPage--">
            <i class="fas fa-chevron-left text-xs"></i>
          </button>
          <button v-for="page in visiblePages" :key="page"
            class="pagination-btn w-8 h-8 rounded flex items-center justify-center border border-gray-200 text-sm transition-all duration-300"
            :class="{
              'bg-blue-500 text-white border-blue-500': currentPage === page,
              'text-gray-700 hover:border-blue-500 hover:text-blue-500': currentPage !== page
            }" @click="currentPage = page">
            {{ page }}
          </button>
          <button
            class="pagination-btn px-3 py-1.5 rounded border border-gray-200 text-gray-500 hover:border-blue-500 hover:text-blue-500 transition-all duration-300"
            :disabled="currentPage === totalPages" @click="currentPage++">
            <i class="fas fa-chevron-right text-xs"></i>
          </button>
        </div>
        <div class="pagination-info text-sm text-gray-500 ml-4 hidden md:block">
          {{ t('bucket.totalBuckets', { count: filteredBuckets.length }) }}
        </div>
      </div>
    </div>

    <!-- 创建桶对话框 -->
    <div v-if="dialogVisible"
      class="dialog-overlay fixed inset-0 bg-[rgba(0,0,0,0.5)] flex items-center justify-center z-50">
      <div class="dialog-container bg-white rounded-xl shadow-xl w-full max-w-md mx-4 overflow-hidden animate-fadeIn">
        <div class="dialog-header p-5 border-b border-gray-100 flex items-center justify-between">
          <h3 class="text-lg font-bold text-gray-800">{{ t('bucket.createBucket') }}</h3>
          <button @click="handleCloseDialog" class="close-button text-gray-500 hover:text-gray-700 transition-colors"
            :aria-label="t('common.close')">
            <i class="fas fa-times"></i>
          </button>
        </div>
        <div class="dialog-body p-5">
          <div class="form-content space-y-4">
            <div class="form-group">
              <label class="form-label block text-sm font-medium text-gray-700 mb-1"> {{ t('bucket.bucketName') }} </label>
              <input v-model="bucketName" type="text" :placeholder="t('bucket.enterBucketName')" :disabled="creating"
                class="form-input w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none" @keyup.enter="handleCreateBucket" />
              <p class="text-xs text-gray-500 mt-1">
                {{ t('bucket.bucketNameValidation') }}
              </p>
            </div>
          </div>
        </div>
        <div class="dialog-footer p-5 border-t border-gray-100 flex items-center justify-end gap-3">
          <button @click="handleCloseDialog"
            class="cancel-button px-4 py-2 border border-gray-300 rounded-lg text-gray-700 hover:bg-gray-50 transition-colors" :disabled="creating">
            {{ t('common.cancel') }}
          </button>
          <button @click="handleCreateBucket"
            class="confirm-button px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors" :disabled="creating">
            {{ creating ? t('bucket.creating') : t('common.create') }}
          </button>
        </div>
      </div>
    </div>

    <!-- 删除确认对话框 -->
    <div v-if="deleteDialogVisible" class="dialog-overlay fixed inset-0 bg-[rgba(0,0,0,0.5)] flex items-center justify-center z-50">
      <div class="dialog-container bg-white rounded-xl shadow-xl w-full max-w-md mx-4 overflow-hidden animate-fadeIn">
        <div class="dialog-header p-5 border-b border-gray-100 flex items-center justify-between">
          <h3 class="text-lg font-bold text-gray-800">{{ t('common.confirmDelete') }}</h3>
          <button @click="deleteDialogVisible = false" class="close-button text-gray-500 hover:text-gray-700 transition-colors" :aria-label="t('common.close')">
            <i class="fas fa-times"></i>
          </button>
        </div>
        <div class="dialog-body p-5">
          <p class="text-gray-700">{{ t('bucket.confirmDeleteMessage', { name: bucketToDelete?.name }) }}</p>
        </div>
        <div class="dialog-footer p-5 border-t border-gray-100 flex items-center justify-end gap-3">
          <button @click="deleteDialogVisible = false" class="cancel-button px-4 py-2 border border-gray-300 rounded-lg text-gray-700 hover:bg-gray-50 transition-colors">
            {{ t('common.cancel') }}
          </button>
          <button @click="confirmDeleteBucket" class="confirm-button px-4 py-2 bg-red-600 text-white rounded-lg hover:bg-red-700 transition-colors">
            {{ t('common.delete') }}
          </button>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue';
import { useRouter } from 'vue-router';
import { useI18n } from 'vue-i18n';
import { listbuckets, createbucket, deletebucket } from '@/api/admin.js';
import { ElMessage } from 'element-plus';

const { t } = useI18n();
const router = useRouter();

// ==================== 响应式数据定义 ====================
const searchQuery = ref('');
const currentPage = ref(1);
const pageSize = ref(10);
const buckets = ref([]);
const loading = ref(false);
const error = ref('');
const dialogVisible = ref(false);
const bucketName = ref('');
const creating = ref(false);
const deleteDialogVisible = ref(false);
const bucketToDelete = ref(null);

// ==================== 计算属性 ====================
const filteredBuckets = computed(() => {
  if (!searchQuery.value.trim()) {
    return buckets.value;
  }

  const query = searchQuery.value.toLowerCase().trim();
  return buckets.value.filter(bucket => bucket.name.toLowerCase().includes(query));
});

const paginatedBuckets = computed(() => {
  const startIndex = (currentPage.value - 1) * pageSize.value;
  const endIndex = startIndex + pageSize.value;
  return filteredBuckets.value.slice(startIndex, endIndex);
});

const totalPages = computed(() => Math.ceil(filteredBuckets.value.length / pageSize.value));

const visiblePages = computed(() => {
  const pages = [];
  const maxVisible = 5;
  let start = Math.max(1, currentPage.value - Math.floor(maxVisible / 2));
  let end = Math.min(totalPages.value, start + maxVisible - 1);

  if (end - start + 1 < maxVisible) {
    start = Math.max(1, end - maxVisible + 1);
  }

  for (let i = start; i <= end; i++) {
    pages.push(i);
  }

  return pages;
});

// ==================== 工具函数 ====================
const formatNumber = (num) => {
  if (num >= 1000000) {
    return (num / 1000000).toFixed(1) + 'M';
  } else if (num >= 1000) {
    return (num / 1000).toFixed(1) + 'K';
  }
  return num;
};

const formatSize = (bytes) => {
  if (bytes >= 1099511627776) {
    return (bytes / 1099511627776).toFixed(2) + 'TB';
  } else if (bytes >= 1073741824) {
    return (bytes / 1073741824).toFixed(2) + 'GB';
  } else if (bytes >= 1048576) {
    return (bytes / 1048576).toFixed(2) + 'MB';
  } else if (bytes >= 1024) {
    return (bytes / 1024).toFixed(2) + 'KB';
  }
  return bytes + 'B';
};

const formatDate = (dateString) => {
  const date = new Date(dateString);
  return date.toLocaleDateString('zh-CN', {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit'
  }).replace(/\//g, '-');
};

// ==================== 桶操作函数 ====================
const fetchBuckets = async () => {
  loading.value = true;
  error.value = '';

  try {
    const result = await listbuckets();

    if (result.success === false) {
      error.value = result.message || t('bucket.failedToListBuckets');
      return;
    }

    // 处理后端返回的数据结构
    const bucketData = result.data || [];
    buckets.value = bucketData.map(bucket => ({
      name: bucket.base?.name || 'unknown',
      objectCount: bucket.stats?.objectCount || 0,
      size: bucket.stats?.sizeOfObject || 0,
      region: bucket.base?.location || 'unknown',
      owner: bucket.base?.owner?.displayName || 'unknown',
      createdAt: bucket.base?.creationDate || new Date().toISOString()
    }));
  } catch (err) {
    console.error('Error fetching buckets:', err);
    error.value = t('common.networkErrorPleaseRetry');
  } finally {
    loading.value = false;
  }
};

const navigateToBucket = (bucketName) => {
  router.push(`/bucket/${bucketName}`);
};

// ==================== 创建桶相关函数 ====================
const openCreateBucketDialog = () => {
  bucketName.value = '';
  dialogVisible.value = true;
};

const validateBucketName = (name) => {
  if (!name || name.trim() === '') {
    return { valid: false, message: t('bucket.bucketNameCannotBeEmpty') };
  }
  if (name.length < 3 || name.length > 63) {
    return { valid: false, message: t('bucket.bucketNameLengthError') };
  }
  // 桶名称只能包含小写字母、数字、连字符，且不能以连字符开头或结尾
  const bucketNameRegex = /^[a-z0-9][a-z0-9\-]{1,61}[a-z0-9]$/;
  if (!bucketNameRegex.test(name)) {
    return { valid: false, message: t('bucket.bucketNameFormatError') };
  }

  return { valid: true };
};

const handleCreateBucket = async () => {
  const validation = validateBucketName(bucketName.value);
  if (!validation.valid) {
    ElMessage.error(validation.message);
    return;
  }

  creating.value = true;
  try {
    const result = await createbucket({
      name: bucketName.value,
      region: 'us-east-1'
    });

    if (result.success === true || result.code === 0) {
      ElMessage.success(t('bucket.bucketCreatedSuccessfully'));
      dialogVisible.value = false;
      await fetchBuckets(); // 重新获取桶列表
    } else {
      // 处理不同的错误情况
      if (result.message === 'BucketAlreadyOwnedByYou' || result.message?.includes('BucketAlreadyOwnedByYou')) {
        ElMessage.error(t('bucket.alreadyOwnThisBucket'));
      } else if (result.message === 'BucketAlreadyExists' || result.message?.includes('BucketAlreadyExists')) {
        ElMessage.error(t('bucket.bucketNameAlreadyExists'));
      } else if (result.message === 'BucketMetadataNotInitialized' || result.message?.includes('BucketMetadataNotInitialized')) {
        ElMessage.error(t('bucket.bucketMetadataInitFailed'));
      } else {
        ElMessage.error(result.message || t('bucket.createBucketFailed'));
      }
    }
  } catch (err) {
    console.error('Error creating bucket:', err);
    ElMessage.error(t('common.networkErrorPleaseRetry'));
  } finally {
    creating.value = false;
  }
};

const handleCloseDialog = () => {
  dialogVisible.value = false;
};

// ==================== 删除桶相关函数 ====================
const handleDeleteBucket = (bucket) => {
  bucketToDelete.value = bucket;
  deleteDialogVisible.value = true;
};

const confirmDeleteBucket = async () => {
  if (!bucketToDelete.value) return;
  
  try {
    const result = await deletebucket({
      name: bucketToDelete.value.name,
      region: bucketToDelete.value.region || 'us-east-1'
    });

    deleteDialogVisible.value = false;

    if (result.success === true || result.code === 0) {
      ElMessage.success(t('bucket.deletedSuccess'));
      await fetchBuckets(); // 重新获取桶列表
    } else {
      // 处理不同的错误情况
      if (result.message === 'NoSuchBucket' || result.message?.includes('NoSuchBucket')) {
        ElMessage.error(t('bucket.noSuchBucket'));
      } else if (result.message === 'BucketNotEmpty' || result.message?.includes('BucketNotEmpty')) {
        ElMessage.error(t('bucket.bucketNotEmpty'));
      } else if (result.message === 'AccessDenied' || result.message?.includes('AccessDenied')) {
        ElMessage.error(t('bucket.accessDeniedToDelete'));
      } else if (result.message === 'BucketMetadataNotInitialized' || result.message?.includes('BucketMetadataNotInitialized')) {
        ElMessage.error(t('bucket.bucketMetadataInitFailed'));
      } else {
        ElMessage.error(result.message || t('bucket.deleteBucketFailed'));
      }
    }
  } catch (err) {
    console.error('Error deleting bucket:', err);
    deleteDialogVisible.value = false;
    ElMessage.error(t('common.networkErrorPleaseRetry'));
  }
};

// ==================== 生命周期 ====================
onMounted(() => {
  fetchBuckets();
});
</script>

<style scoped>
/* 主要容器样式 */
.buckets-container {
  min-height: 100%;
  background: linear-gradient(135deg, #f5f7fa 0%, #e4e8f0 100%);
  padding: 1rem;
}

/* 头部操作区域 */
.header-actions {
  display: flex;
  flex-direction: column;
  align-items: flex-start;
  justify-content: space-between;
  margin-bottom: 0.5rem;
  gap: 0.5rem;
  transition: all 0.3s ease;
}

@media (min-width: 640px) {
  .header-actions {
    flex-direction: row;
    align-items: center;
    margin-bottom: 0.75rem;
    gap: 0.5rem;
  }
}

.page-title {
  display: flex;
  flex-direction: column;
  align-items: flex-start;
  gap: 0.25rem;
}

@media (min-width: 640px) {
  .page-title {
    flex-direction: row;
    align-items: center;
    gap: 0.5rem;
  }
}

.page-title h1 {
  background: linear-gradient(90deg, #409EFF, #67C23A);
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
  background-clip: text;
}

/* 搜索和创建按钮容器 */
.search-create-container {
  display: flex;
  flex-direction: column;
  gap: 0.5rem;
  width: 100%;
}

@media (min-width: 640px) {
  .search-create-container {
    flex-direction: row;
    width: auto;
  }
}

/* 桶列表容器 */
.buckets-list-container {
  position: relative;
  overflow: hidden;
}

/* 桶卡片样式 */
.bucket-item {
  transition: all 0.3s ease;
}

.bucket-item:hover {
  transform: translateY(-2px);
  box-shadow: 0 10px 15px -5px rgba(0, 0, 0, 0.1), 0 5px 5px -5px rgba(0, 0, 0, 0.04);
}

/* 桶标题样式 */
.bucketTitle {
  border-bottom: 1px solid #f0f0f0;
  display: flex;
  align-items: center;
  justify-content: space-between;
  width: 100%;
  padding: 1rem;
}

.bucketTitle h1 {
  font-weight: 600;
  font-size: 1.5rem;
  color: #111827;
  margin: 0;
  flex-grow: 1;
  margin-right: 1rem;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

/* 桶详情样式 */
.bucketDetails {
  color: #4b5563;
  font-size: 0.875rem;
  padding: 0 1rem 0.75rem;
}

.bucketDetails strong {
  font-weight: 500;
}

/* 桶指标样式 */
.bucketMetrics {
  border-top: 1px solid #f0f0f0;
  padding: 1rem;
}

/* 桶图标样式 */
.bucketIcon {
  color: #3b82f6;
  transition: transform 0.3s ease;
}

.bucketIcon:hover {
  transform: scale(1.1);
}

/* 指标项样式 */
.metric {
  display: flex;
  align-items: center;
  gap: 0.5rem;
}

/* 指标图标样式 */
.min-icon {
  height: 1.25rem;
  width: 1.25rem;
  color: #6b7280;
}

/* 指标标签样式 */
.metricLabel {
  font-size: 0.75rem;
  color: #6b7280;
  font-weight: 500;
}

/* 指标文本样式 */
.metricText {
  font-weight: 600;
  color: #111827;
  font-size: 1rem;
}

/* 操作按钮样式 */
.actions {
  display: flex;
  gap: 0.5rem;
  flex-shrink: 0;
  justify-content: flex-end;
  align-items: center;
}

.action-btn {
  position: relative;
  overflow: hidden;
  width: 2rem;
  height: 2rem;
  display: flex;
  align-items: center;
  justify-content: center;
  border-radius: 0.5rem;
  transition: all 0.3s ease;
  flex-shrink: 0;
}

.action-btn:hover {
  transform: translateY(-2px);
}

.action-btn:active {
  transform: translateY(0);
}

/* 分页按钮样式 */
.pagination-btn {
  transition: all 0.3s ease;
}

.pagination-btn:hover:not(:disabled) {
  transform: translateY(-1px);
}

.pagination-btn:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

.pagination-btn:disabled:hover {
  transform: none;
  border-color: #dcdfe6;
  color: #c0c4cc;
}

/* 空状态样式 */
.empty-state {
  animation: fadeIn 0.6s ease-out forwards;
}

/* 加载状态样式 */
.loading-state {
  animation: fadeIn 0.6s ease-out forwards;
}

.loading-state h3 {
  font-weight: 600;
  color: #111827;
  margin: 0 0 0.5rem 0;
}

.loading-state p {
  color: #6b7280;
  margin: 0;
}

/* 错误状态样式 */
.error-state {
  animation: fadeIn 0.6s ease-out forwards;
}

.error-state h3 {
  font-weight: 600;
  color: #111827;
  margin: 0 0 0.5rem 0;
}

.error-state p {
  color: #6b7280;
  margin: 0 0 1.5rem 0;
}

.retry-btn {
  box-shadow: 0 4px 12px rgba(64, 158, 255, 0.2);
  transition: all 0.3s ease;
}

.retry-btn:hover {
  box-shadow: 0 6px 16px rgba(64, 158, 255, 0.3);
  transform: translateY(-2px);
}

.retry-btn:active {
  transform: translateY(0);
}

/* 对话框样式 */
.dialog-overlay {
  animation: fadeIn 0.3s ease-out;
}

.dialog-container {
  animation: slideInUp 0.3s ease-out;
}

.dialog-header h3 {
  margin: 0;
}

.dialog-header .close-button {
  padding: 0;
  border: none;
  background: none;
  font-size: 1.25rem;
  cursor: pointer;
  display: flex;
  align-items: center;
  justify-content: center;
  width: 32px;
  height: 32px;
  border-radius: 4px;
  transition: all 0.2s ease;
}

.dialog-header .close-button:hover {
  background-color: #f5f5f5;
}

.form-content {
  width: 100%;
}

.form-group {
  margin-bottom: 1rem;
}

.form-label {
  display: block;
  margin-bottom: 0.25rem;
  font-weight: 500;
  color: #333;
}

.form-input {
  width: 100%;
  padding: 0.5rem 0.75rem;
  border: 1px solid #ddd;
  border-radius: 0.5rem;
  font-size: 0.875rem;
  transition: all 0.3s ease;
}

.form-input:focus {
  outline: none;
  border-color: #409EFF;
  box-shadow: 0 0 0 2px rgba(64, 158, 255, 0.2);
}

.form-input:disabled {
  background-color: #f5f5f5;
  cursor: not-allowed;
  opacity: 0.6;
}

.cancel-button {
  padding: 0.5rem 1rem;
  border: 1px solid #ddd;
  border-radius: 0.5rem;
  background-color: #fff;
  color: #666;
  font-size: 0.875rem;
  cursor: pointer;
  transition: all 0.3s ease;
}

.cancel-button:hover:not(:disabled) {
  background-color: #f5f5f5;
  border-color: #ccc;
}

.cancel-button:disabled {
  opacity: 0.6;
  cursor: not-allowed;
}

.confirm-button {
  padding: 0.5rem 1rem;
  border: none;
  border-radius: 0.5rem;
  background-color: #409EFF;
  color: #fff;
  font-size: 0.875rem;
  cursor: pointer;
  transition: all 0.3s ease;
}

.confirm-button:hover:not(:disabled) {
  background-color: #368ce7;
  box-shadow: 0 2px 8px rgba(64, 158, 255, 0.3);
}

.confirm-button:disabled {
  opacity: 0.6;
  cursor: not-allowed;
}

/* 动画效果 */
@keyframes fadeIn {
  from {
    opacity: 0;
    transform: translateY(10px);
  }

  to {
    opacity: 1;
    transform: translateY(0);
  }
}

@keyframes slideInUp {
  from {
    transform: translateY(20px);
    opacity: 0;
  }
  to {
    transform: translateY(0);
    opacity: 1;
  }
}

.animate-fadeIn {
  animation: fadeIn 0.6s ease-out forwards;
  opacity: 0;
}

/* 响应式设计 */
@media (max-width: 639px) {
  .buckets-container {
    padding: 0.75rem;
  }

  .header-actions {
    flex-direction: column;
    align-items: stretch;
    margin-bottom: 0.75rem;
    gap: 1rem;
  }

  .page-title {
    margin-bottom: 0.5rem;
    flex-direction: row !important;
    align-items: center !important;
    gap: 0.5rem !important;
  }

  .page-title h1 {
    font-size: 1.25rem;
    line-height: 1.2;
    max-width: calc(100% - 80px);
  }

  .page-title p {
    font-size: 0.75rem;
    line-height: 1.2;
    opacity: 0.9;
  }

  .search-create-container {
    width: 100%;
    gap: 0.75rem;
  }

  .pagination-info {
    display: none;
  }

  .pagination-controls {
    margin: 0 auto;
    justify-content: center;
  }

  .bucketMetrics .flex {
    width: 100%;
    align-items: center;
  }
}

@media (max-width: 480px) {
  .buckets-container {
    padding: 0.5rem;
  }

  .header-actions {
    margin-bottom: 0.5rem;
    gap: 0.75rem;
  }

  .page-title {
    flex-wrap: wrap;
    margin-bottom: 0.25rem;
  }

  .page-title h1 {
    font-size: 1.125rem;
    line-height: 1.2;
  }

  .page-title p {
    font-size: 0.7rem;
    opacity: 0.85;
    min-width: 100%;
    margin-top: 0.25rem;
  }

  .search-create-container {
    gap: 0.5rem;
  }

  .create-bucket-btn {
    padding: 0.625rem 1.25rem !important;
    font-size: 0.8125rem !important;
    white-space: nowrap;
  }
}

@media (max-width: 380px) {
  .header-actions {
    margin-bottom: 0.5rem;
    gap: 0.5rem;
  }

  .page-title p {
    display: none;
  }

  .search-create-container {
    flex-direction: column;
    gap: 0.5rem;
  }

  .search-create-container input {
    padding: 0.625rem 0.75rem !important;
  }

  .create-bucket-btn {
    padding: 0.625rem 1.25rem !important;
    width: 100%;
  }
}
</style>