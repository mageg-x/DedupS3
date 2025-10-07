<template>
  <div class="browser-container">
    <!-- 头部导航区域 -->
    <div
      class="header-nav flex flex-col sm:flex-row items-start sm:items-center justify-between mb-2 sm:mb-3 gap-1 sm:gap-2 transition-all duration-300">
      <div class="nav-path flex items-center">
        <button @click="backToList"
          class="back-btn p-2 text-gray-500 hover:text-blue-600 hover:bg-blue-50 rounded-lg transition-all duration-300 mr-2">
          <i class="fas fa-arrow-left"></i>
        </button>
        <h1 class="text-xl font-bold text-gray-800 transition-all duration-300 sm:text-2xl m-0">
          {{ bucketName }}
        </h1>
        <div class="breadcrumb ml-3 text-sm text-gray-500">
          <a href="#" class="hover:text-blue-600">{{ t('common.buckets') }}</a>
          <span class="ml-1">/</span>
          <span class="text-gray-700">{{ bucketName }}</span>
          <span v-if="currentPath !== ''">/</span>
          <span v-if="currentPath !== ''" class="text-gray-700 truncate">
            {{ currentPath }}
          </span>
        </div>
      </div>

      <div class="header-actions flex items-center gap-2">
        <button
          class="upload-btn bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded-lg font-medium flex items-center justify-center transition-all duration-300 transform hover:scale-105 hover:shadow-md whitespace-nowrap text-sm"
          @click="handleUploadClick" :disabled="uploading.value">
          <i v-if="uploading" class="fas fa-spinner fa-spin mr-1.5 text-sm"></i>
          <i v-else class="fas fa-upload mr-1.5 text-sm"></i>
          {{ t('browser.uploadFile') }}
          <span v-if="uploading" class="ml-2">{{ uploadProgress }}%</span>
        </button>
        <button
          class="create-folder-btn bg-gray-600 hover:bg-gray-700 text-white px-4 py-2 rounded-lg font-medium flex items-center justify-center transition-all duration-300 transform hover:scale-105 hover:shadow-md whitespace-nowrap text-sm"
          @click="openCreateFolderDialog">
          <i class="fas fa-folder-plus mr-1.5 text-sm"></i>
          {{ t('browser.createFolder') }}
        </button>
      </div>
    </div>

    <!-- 主内容区域（列表 + 侧边栏） -->
    <div class="main-content flex h-[calc(100vh-120px)]">
      <!-- 对象列表区域 -->
      <div class="objects-list-container bg-white rounded-l-xl shadow-md border border-gray-100 overflow-hidden flex-1">
        <!-- 列表头部 -->
        <div
          class="list-header grid grid-cols-12 px-4 py-3 border-b border-gray-200 bg-gray-50 text-sm font-medium text-gray-500">
          <div class="col-span-1 sm:col-span-1 flex items-center justify-center">
            <input type="checkbox" :checked="isAllSelected" @change="toggleSelectAll"
              class="rounded text-blue-600 focus:ring-blue-500">
          </div>
          <div class="col-span-5 sm:col-span-3">{{ t('common.name') }}</div>
          <div class="hidden sm:block col-span-2">{{ t('common.type') }}</div>
          <div class="hidden lg:block col-span-2">{{ t('common.size') }}</div>
          <div class="col-span-4 sm:col-span-4 lg:col-span-4">{{ t('common.modifiedDate') }}</div>
        </div>

        <!-- 对象列表 -->
        <div class="objects-list">
          <!-- 返回上一级 -->
          <div v-if="currentPath !== ''" @click="navigateUp"
            class="object-item flex items-center grid grid-cols-12 px-4 py-3 hover:bg-gray-50 transition-all duration-300 cursor-pointer animate-fadeIn">
            <div class="col-span-1 sm:col-span-1 flex items-center justify-center">
              <span class="text-gray-300">•</span>
            </div>
            <div class="col-span-5 sm:col-span-3 flex items-center">
              <i class="fas fa-folder-open text-gray-400 mr-3 text-lg"></i>
              <span class="font-medium text-gray-700">..</span>
            </div>
            <div class="hidden sm:flex col-span-2 items-center justify-start">
              <span class="text-gray-500">{{ t('common.folder') }}</span>
            </div>
            <div class="hidden lg:flex col-span-2 items-center justify-start">
              <span class="text-gray-500">-</span>
            </div>
            <div class="col-span-4 sm:col-span-4 lg:col-span-4 flex justify-end sm:justify-start sm:pl-4">
              <span class="text-gray-500 text-sm">{{ formatDate(getParentDirLastModified()) }}</span>
            </div>
          </div>

          <!-- 文件夹列表 -->
          <div v-for="folder in currentFolders" :key="folder.name"
            class="object-item flex items-center grid grid-cols-12 px-4 py-3 hover:bg-gray-50 transition-all duration-300 cursor-pointer animate-fadeIn"
            :style="{ animationDelay: (currentPath === '' ? 0 : 1) + (folder.index * 0.05) + 's' }">
            <div class="col-span-1 sm:col-span-1 flex items-center justify-center">
              <input type="checkbox" :checked="isSelected(folder)" @change.stop="toggleSelect(folder)"
                class="rounded text-blue-600 focus:ring-blue-500">
            </div>
            <div class="col-span-5 sm:col-span-3 flex items-center" @click.stop="navigateToFolder(folder)">
              <i class="fas fa-folder text-blue-500 mr-3 text-lg"></i>
              <span class="font-medium text-gray-700">{{ folder.name }}</span>
            </div>
            <div class="hidden sm:flex col-span-2 items-center justify-start">
              <span class="text-gray-500">{{ t('common.folder') }}</span>
            </div>
            <div class="hidden lg:flex col-span-2 items-center justify-start">
              <span class="text-gray-500">-</span>
            </div>
            <div class="col-span-4 sm:col-span-4 lg:col-span-4 flex justify-end sm:justify-start sm:pl-4">
              <span class="text-gray-500 text-sm">{{ formatDate(folder.lastModified) }}</span>
            </div>
          </div>

          <!-- 文件列表 -->
          <div v-for="file in currentFiles" :key="file.name" @click.stop="selectFile(file)"
            class="object-item flex items-center grid grid-cols-12 px-4 py-3 hover:bg-gray-50 transition-all duration-300 cursor-pointer animate-fadeIn"
            :style="{ animationDelay: (currentPath === '' ? 0 : 1) + (foldersCount + file.index) * 0.05 + 's' }">
            <div class="col-span-1 sm:col-span-1 flex items-center justify-center">
              <input type="checkbox" v-model="file.isSelected"
                class="rounded text-blue-600 focus:ring-blue-500">
            </div>
            <div class="col-span-5 sm:col-span-3 flex items-center">
              <i :class="getFileIconClass(file.name)" class="mr-3 text-lg"></i>
              <span class="font-medium text-gray-700 truncate">{{ file.name }}</span>
            </div>
            <div class="hidden sm:flex col-span-2 items-center justify-start">
              <span class="text-gray-500">{{ getFileExtension(file.name) }}</span>
            </div>
            <div class="hidden lg:flex col-span-2 items-center justify-start">
              <span class="text-gray-500">{{ formatSize(file.size) }}</span>
            </div>
            <div class="col-span-4 sm:col-span-4 lg:col-span-4 flex justify-end sm:justify-start sm:pl-4">
              <span class="text-gray-500 text-sm">{{ formatDate(file.lastModified) }}</span>
            </div>
          </div>

          <!-- 加载状态 -->
          <div v-if="loading" class="loading-state py-16 text-center bg-white rounded-xl shadow-md">
            <div class="w-12 h-12 border-4 border-blue-200 border-t-blue-600 rounded-full animate-spin mx-auto mb-4">
            </div>
            <h3 class="text-xl font-semibold text-gray-800 mb-2">{{ t('common.loading') }}</h3>
            <p class="text-gray-500">{{ t('browser.loadingObjects') }}</p>
          </div>

          <!-- 错误状态 -->
          <div v-else-if="error" class="error-state py-16 text-center bg-white rounded-xl shadow-md">
            <div class="w-16 h-16 bg-red-50 rounded-full flex items-center justify-center mx-auto mb-4">
              <i class="fas fa-exclamation-circle text-3xl text-red-500"></i>
            </div>
            <h3 class="text-xl font-semibold text-gray-800 mb-2">{{ t('common.error') }}</h3>
            <p class="text-gray-500 mb-6 max-w-md mx-auto">{{ error }}</p>
            <button
              class="retry-btn bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded-lg font-medium flex items-center justify-center transition-all duration-300 mx-auto"
              @click="fetchObjects">
              <i class="fas fa-sync-alt mr-2"></i>
              {{ t('common.retry') }}
            </button>
          </div>

          <!-- 空状态 -->
          <div v-else-if="currentFolders.length === 0 && currentFiles.length === 0"
            class="empty-state py-16 text-center">
            <div class="w-24 h-24 bg-gray-100 rounded-full flex items-center justify-center mx-auto mb-4">
              <svg class="w-12 h-12 text-gray-400" viewBox="0 0 256 256" fill="currentColor"
                xmlns="http://www.w3.org/2000/svg">
                <g>
                  <path
                    d="M244.1,8.4c-3.9-5.3-10.1-8.5-16.7-8.5H21.6C15,0,8.8,3.1,4.9,8.4C0.8,14-0.9,21,0.3,27.9c5.1,29.6,15.8,91.9,24.3,141.7v0.1C29,195,32.8,217.1,35,229.9c1.4,10.8,10.4,18.9,21.3,19.3h136.5c10.9-0.4,19.9-8.5,21.3-19.3l10.3-60.1l0.1-0.4L238.4,88v-0.2l10.3-59.9C249.9,21,248.3,14,244.1,8.4 M206.1,177h-163l-3.2-18.6h169.3L206.1,177z M220,95.3H28.9l-3.2-18.6h197.4L220,95.3z" />
                </g>
              </svg>
            </div>
            <h3 class="text-xl font-semibold text-gray-800 mb-2">{{ t('browser.folderEmpty') }}</h3>
            <p class="text-gray-500 mb-6 max-w-md mx-auto">{{ currentPath.value === '' ? t('browser.emptyBucketHint') :
              t('browser.emptyFolderHint') }}</p>
            <button
              class="upload-btn bg-blue-600 hover:bg-blue-700 text-white px-5 py-2.5 rounded-lg font-medium flex items-center justify-center transition-all duration-300 mx-auto transform hover:scale-105 hover:shadow-md"
              @click="handleUploadClick">
              <i class="fas fa-upload mr-2"></i>
              {{ t('browser.uploadFirstFile') }}
            </button>
          </div>
        </div>
      </div>

      <!-- 对象详情侧边栏 -->
      <div v-if="selectedFile"
        class="object-sidebar w-full sm:w-96 bg-white shadow-lg transform transition-transform duration-300 ease-in-out show border-l border-gray-100 overflow-hidden">
        <div class="sidebar-header flex items-center justify-between p-4 border-b border-gray-200">
          <h2 class="text-lg font-semibold text-gray-800 truncate">{{ t('browser.fileDetails') }}</h2>
          <button
            class="close-btn p-2 text-gray-500 hover:text-gray-700 hover:bg-gray-100 rounded-lg transition-all duration-300"
            @click="closeSidebar">
            <i class="fas fa-times"></i>
          </button>
        </div>

        <div class="sidebar-content p-4 overflow-y-auto h-[calc(100%-64px)]">
          <!-- 多对象选择状态 -->
          <div v-if="isMultipleSelection" class="multi-selection-state text-center py-8">
            <div
              class="selection-icon w-20 h-20 bg-blue-100 rounded-full flex items-center justify-center mx-auto mb-4">
              <i class="fas fa-check-circle text-blue-600 text-4xl"></i>
            </div>
            <h3 class="font-semibold text-gray-800 text-lg mb-2">{{ t('browser.selectedObjectsCount', {
              count:
                selectedFiles.length
            }) }}</h3>
            <p class="text-gray-500 text-sm mb-6">{{ t('browser.selectedObjectsHint') }}</p>
          </div>

          <!-- 单文件预览 -->
          <div v-else class="file-preview mb-6">
            <div class="preview-icon w-20 h-20 bg-gray-100 rounded-lg flex items-center justify-center mx-auto mb-2">
              <i :class="getFileIconClass(selectedFile.name)" class="text-4xl"></i>
            </div>
            <div class="text-center">
              <h3 class="font-medium text-gray-800 truncate">{{ selectedFile.name }}</h3>
              <p class="text-gray-500 text-sm">{{ selectedFile.isFolder ? '-' : formatSize(selectedFile.size) }}</p>
            </div>
          </div>

          <!-- 操作按钮 -->
          <div class="action-buttons flex justify-center flex-wrap gap-2 mb-6">
            <button
              class="download-btn flex-1 sm:flex-none bg-green-600 hover:bg-green-700 text-white px-4 py-2.25 rounded-lg font-medium flex items-center justify-center transition-all duration-300 transform hover:scale-105 hover:shadow-md whitespace-nowrap text-sm"
              @click="handleDownload"
              :disabled="!selectedFiles.length">
              <i class="fas fa-download mr-1.5 text-sm"></i>
              {{ t('common.download') }}
            </button>
            <button v-if="selectedFiles.length <= 1"
              class="preview-btn flex-1 sm:flex-none bg-blue-600 hover:bg-blue-700 text-white px-4 py-2.25 rounded-lg font-medium flex items-center justify-center transition-all duration-300 transform hover:scale-105 hover:shadow-md whitespace-nowrap text-sm">
              <i class="fas fa-eye mr-1.5 text-sm"></i>
              {{ t('common.preview') }}
            </button>
            <button
              class="delete-btn flex-1 sm:flex-none bg-red-600 hover:bg-red-700 text-white px-4 py-2.25 rounded-lg font-medium flex items-center justify-center transition-all duration-300 transform hover:scale-105 hover:shadow-md whitespace-nowrap text-sm"
              @click="handleDelete" :disabled="deleting">
              <i v-if="deleting" class="fas fa-spinner fa-spin mr-1.5 text-sm"></i>
              <i v-else class="fas fa-trash-alt mr-1.5 text-sm"></i>
              {{ t('common.delete') }}
            </button>
          </div>

          <!-- 单对象信息 -->
          <div v-if="!isMultipleSelection" class="object-info">
            <h4 class="text-sm font-semibold text-gray-500 uppercase mb-3">{{ t('browser.objectInfo') }}</h4>
            <div class="info-grid grid grid-cols-1 gap-3">
              <div class="info-item">
                <label class="text-xs text-gray-500 block mb-1">{{ t('common.name') }}</label>
                <p class="text-gray-800 font-medium truncate">{{ selectedFile.name }}</p>
              </div>
              <div class="info-item">
                <label class="text-xs text-gray-500 block mb-1">{{ t('common.size') }}</label>
                <p class="text-gray-800">{{ selectedFile.isFolder ? '-' : formatSize(selectedFile.size) + ' (' +
                  selectedFile.size + ' bytes)' }}</p>
              </div>
              <div class="info-item">
                <label class="text-xs text-gray-500 block mb-1">{{ t('common.createdAt') }}</label>
                <p class="text-gray-800">{{ selectedFile.isFolder ? '-' : formatDateTime(selectedFile.createdAt) }}</p>
              </div>
              <div class="info-item">
                <label class="text-xs text-gray-500 block mb-1">{{ t('common.lastModified') }}</label>
                <p class="text-gray-800">{{ selectedFile.isFolder ? '-' : formatDateTime(selectedFile.lastModified) }}
                </p>
              </div>
              <div class="info-item">
                <label class="text-xs text-gray-500 block mb-1">{{ t('browser.etag') }}</label>
                <p class="text-gray-800 text-xs break-all">{{ selectedFile.isFolder ? '-' : selectedFile.etag }}</p>
              </div>
              <div class="info-item" v-if="selectedFile.tags && selectedFile.tags.length > 0">
                <label class="text-xs text-gray-500 block mb-1">{{ t('common.tags') }}</label>
                <div class="tags flex flex-wrap gap-1">
                  <span v-for="tag in selectedFile.tags" :key="tag.key"
                    class="inline-block bg-blue-50 text-blue-600 px-2 py-0.5 rounded text-xs font-medium">
                    {{ tag.key }}: {{ tag.value }}
                  </span>
                </div>
              </div>
              <div class="info-item">
                <label class="text-xs text-gray-500 block mb-1">{{ t('browser.chunkCount') }}</label>
                <p class="text-gray-800">{{ selectedFile.isFolder ? '-' : (selectedFile.chunks || 1) }}</p>
              </div>
              <div class="info-item">
                <label class="text-xs text-gray-500 block mb-1">{{ t('browser.storagePath') }}</label>
                <p class="text-gray-800 text-xs truncate">{{ bucketName }}/{{ currentPath ? currentPath + '/' : '' }}{{
                  selectedFile.name }}</p>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>

    <!-- 文件上传对话框 -->
    <input ref="fileInput" type="file" style="display: none;" @change="handleFileSelect" multiple>

    <!-- 创建文件夹对话框 -->
    <div v-if="createFolderDialogVisible"
      class="fixed inset-0 bg-[rgba(0,0,0,0.5)] flex items-center justify-center z-50 p-4">
      <div class="bg-white rounded-xl shadow-lg w-full max-w-md overflow-hidden transform transition-all duration-300">
        <div class="dialog-header px-6 py-4 border-b border-gray-200 flex items-center justify-between">
          <h3 class="text-lg font-semibold text-gray-800">{{ t('browser.createNewFolder') }}</h3>
          <button @click="closeCreateFolderDialog"
            class="text-gray-500 hover:text-gray-700 p-1 rounded-full hover:bg-gray-100">
            <i class="fas fa-times"></i>
          </button>
        </div>
        <div class="dialog-body px-6 py-5">
          <div class="mb-4">
            <label for="folderName" class="block text-sm font-medium text-gray-700 mb-1">
              {{ t('browser.folderName') }}
            </label>
            <input id="folderName" v-model="folderName" type="text"
              class="w-full px-3 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500 text-gray-700"
              :placeholder="t('browser.enterFolderName')" @keyup.enter="handleCreateFolder">
          </div>
          <div v-if="createFolderError" class="text-red-500 text-sm mb-2">
            {{ createFolderError }}
          </div>
          <div class="text-gray-500 text-sm">
            {{ t('browser.createFolderHint', { path: currentPath === '' ? bucketName : `${bucketName}/${currentPath}` })
            }}
          </div>
        </div>
        <div class="dialog-footer px-6 py-4 bg-gray-50 flex justify-end gap-3 border-t border-gray-200">
          <button @click="closeCreateFolderDialog"
            class="px-4 py-2 border border-gray-300 rounded-lg text-gray-700 hover:bg-gray-100 transition-colors duration-200">
            {{ t('common.cancel') }}
          </button>
          <button @click="handleCreateFolder" :disabled="!folderName.trim() || creatingFolder"
            class="px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white rounded-lg transition-colors duration-200 disabled:opacity-50 disabled:cursor-not-allowed">
            <i v-if="creatingFolder" class="fas fa-spinner fa-spin mr-2"></i>
            {{ t('common.create') }}
          </button>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, watch } from 'vue';
import { useRoute, useRouter } from 'vue-router';
import { useI18n } from 'vue-i18n';
import { listobjects, createfolder, putobject, delobject, getobject } from '@/api/admin.js';
import { ElMessage } from 'element-plus';

const { t } = useI18n();

const route = useRoute();
const router = useRouter();

// 获取路由参数中的桶名
const bucketName = ref(route.params.name || '');

// 当前路径
const currentPath = ref('');

// 选中的文件
const selectedFile = ref(null);

// 选中的多个文件
const selectedFiles = ref([]);

// 是否全选
const isAllSelected = ref(false);

// 创建文件夹相关状态
const createFolderDialogVisible = ref(false);
const folderName = ref('');
const createFolderError = ref('');
const creatingFolder = ref(false);

// 上传相关状态
const uploading = ref(false);
const uploadProgress = ref(0);
const uploadError = ref('');

// 存储从API获取的对象列表
const objectsList = ref([]);

// 加载状态
const loading = ref(false);

// 错误信息
const error = ref('');

// 分页相关
const nextMarker = ref('');

// 处理从API获取的数据，转换为前端需要的格式
const processObjectsData = (data) => {
  const folders = [];
  const files = [];

  if (data?.objects) {
    data.objects.forEach((item, idx) => {
      if (item.isFolder) {
        // 处理文件夹 - 保留完整路径信息
        const folderName = item.name.split('/').filter(p => p).pop();
        folders.push({
          name: folderName, // 显示名称
          fullPath: item.name, // 完整路径
          originalItem: item, // 原始数据引用
          lastModified: item.lastModify ? item.lastModify : new Date().toISOString(),
          index: idx,
          isFolder: true // 标记为文件夹
        });
      } else {
        // 处理文件
        // 从文件名中提取名称（去掉路径部分）
        const fileName = item.name.split('/').pop();

        // 转换tags格式为前端需要的数组格式
        let tags = [];
        if (item.tags && typeof item.tags === 'object') {
          tags = Object.entries(item.tags).map(([key, value]) => ({
            key,
            value
          }));
        }

        files.push({
          name: fileName, // 显示名称
          fullPath: item.name, // 完整路径
          originalItem: item, // 原始数据引用
          size: item.size || 0,
          lastModified: item.lastModify ? item.lastModify : new Date().toISOString(),
          createdAt: item.createdAt ? item.createdAt : new Date().toISOString(),
          etag: item.etag || '',
          tags: tags,
          chunks: item.chunks ? item.chunks.length : 1,
          index: idx
        });
      }
    });
  }

  // 更新分页标记
  nextMarker.value = data?.nextMarker || '';

  return { folders, files };
};

// 当前目录下的文件夹
const currentFolders = computed(() => {
  const data = processObjectsData(objectsList.value);
  return data.folders;
});

// 当前目录下的文件
const currentFiles = computed(() => {
  const data = processObjectsData(objectsList.value);
  return data.files;
});

// 文件夹数量
const foldersCount = computed(() => {
  return currentFolders.value.length;
});

// 计算属性：是否为多选状态
const isMultipleSelection = computed(() => {
  return selectedFiles.value.length > 1;
});

// 文件输入引用
const fileInput = ref(null);

// 删除相关状态
const deleting = ref(false);

// 处理上传按钮点击
const handleUploadClick = () => {
  fileInput.value?.click();
};

// 处理文件选择
const handleFileSelect = async (event) => {
  const files = event.target.files;
  if (!files || files.length === 0) return;

  for (let i = 0; i < files.length; i++) {
    await uploadFile(files[i]);
  }

  // 清空文件输入，以便可以重复选择同一个文件
  event.target.value = '';
};

// 上传文件
const uploadFile = async (file) => {
  if (!bucketName.value) {
    ElMessage.error(t('browser.noBucketSelected'));
    return;
  }

  uploading.value = true;
  uploadError.value = '';
  uploadProgress.value = 0;

  try {
    // 构建完整的对象名称（包含路径）
    const objectName = currentPath.value
      ? `${currentPath.value}/${file.name}`
      : file.name;

    // 创建FormData
    const formData = new FormData();
    formData.append('file', file);
    formData.append('bucket', bucketName.value);
    formData.append('object', objectName);
    formData.append('contentType', file.type || 'application/octet-stream');

    // 上传文件
    const result = await putobject(formData, {
      onUploadProgress: (progressEvent) => {
        if (progressEvent.total) {
          uploadProgress.value = Math.round((progressEvent.loaded * 100) / progressEvent.total);
        }
      }
    });

    // 处理响应
    if (result.success === true || result.code === 0) {
      ElMessage.success(t('browser.uploadSuccess', { fileName: file.name }));
      // 上传成功后刷新文件列表
      await fetchObjects();
    } else {
      // 处理不同的错误情况
      if (result.message === 'NoSuchBucket' || result.message?.includes('NoSuchBucket')) {
        ElMessage.error(t('browser.bucketNotFound'));
      } else if (result.message === 'AccessDenied' || result.message?.includes('AccessDenied')) {
        ElMessage.error(t('common.accessDenied'));
      } else {
        ElMessage.error(result.message || t('browser.uploadFailed'));
      }
    }
  } catch (error) {
    console.error('Error uploading file:', error);
    ElMessage.error(t('browser.uploadFailed'));
  } finally {
    uploading.value = false;
    uploadProgress.value = 0;
  }
};

// 返回上一级
const navigateUp = () => {
  if (currentPath.value === '') return;
  console.log('currentPath 1:', currentPath.value);
  const pathParts = currentPath.value.split('/');
  let s = pathParts.pop();
  if (s === null || s === undefined || s === "") {
    pathParts.pop();
  }
  currentPath.value = pathParts.join('/');
  console.log('currentPath 2:', currentPath.value);
  // 导航回上一级后立即获取该文件夹下的对象
  fetchObjects();
};

// 导航到子文件夹
const navigateToFolder = (folder) => {
  currentPath.value = folder.fullPath;

  // 进入文件夹内部时关闭详情侧边栏
  selectedFile.value = null;

  // 导航到文件夹后立即获取该文件夹下的对象
  fetchObjects();
};

// 选择单个文件
const selectFile = (file) => {
  selectedFile.value = file;
  // 切换文件的isSelected状态
  if (file.hasOwnProperty('isSelected')) {
    file.isSelected = !file.isSelected;
    // 根据isSelected状态更新selectedFiles数组
    const index = selectedFiles.value.findIndex(f => f.fullPath === file.fullPath);
    if (file.isSelected && index === -1) {
      selectedFiles.value.push(file);
    } else if (!file.isSelected && index > -1) {
      selectedFiles.value.splice(index, 1);
    }
  }
};

// 切换选择状态
const toggleSelect = (item) => {
  // 使用fullPath进行比较，确保正确识别不同路径下的同名文件
  const index = selectedFiles.value.findIndex(f => f.fullPath === item.fullPath);
  if (index > -1) {
    selectedFiles.value.splice(index, 1);
    // 对于文件，更新其isSelected状态
    if (item.hasOwnProperty('isSelected')) {
      item.isSelected = false;
    }
  } else {
    selectedFiles.value.push(item);
    // 对于文件，更新其isSelected状态
    if (item.hasOwnProperty('isSelected')) {
      item.isSelected = true;
    }
  }

  // 如果只选择了一个文件，显示其详情
  if (selectedFiles.value.length === 1) {
    selectedFile.value = selectedFiles.value[0];
  } else if (selectedFiles.value.length > 1) {
    // 选择多个文件时，显示自定义标题
    selectedFile.value = {
      name: `${selectedFiles.value.length} 个对象已选择`
    };
  } else {
    selectedFile.value = null;
  }

  // 更新全选状态
  updateSelectAllState();
};

// 初始化文件的isSelected状态
const initializeFileSelectionState = () => {
  // 确保每个文件都有isSelected属性，并根据selectedFiles数组设置其状态
  if (currentFiles.value && currentFiles.value.length > 0) {
    currentFiles.value.forEach(file => {
      file.isSelected = isSelected(file);
    });
  }
};

// 监听currentFiles中的isSelected变化，同步更新selectedFiles数组
watch(() => currentFiles.value, (newFiles) => {
  if (newFiles && newFiles.length > 0) {
    // 为每个文件添加isSelected属性的响应式监听
    newFiles.forEach(file => {
      if (file.hasOwnProperty('isSelected')) {
        watch(() => file.isSelected, (newValue) => {
          const index = selectedFiles.value.findIndex(f => f.fullPath === file.fullPath);
          if (newValue && index === -1) {
            selectedFiles.value.push(file);
          } else if (!newValue && index > -1) {
            selectedFiles.value.splice(index, 1);
          }
          updateSelectAllState();
        }, { immediate: false });
      }
    });
  }
}, { deep: true, immediate: true });

// 检查是否被选中
const isSelected = (item) => {
  // 使用fullPath进行比较，确保正确识别不同路径下的同名文件
  return selectedFiles.value.some(f => f.fullPath === item.fullPath);
};

// 切换全选
const toggleSelectAll = () => {
  if (isAllSelected.value) {
    // 取消全选
    selectedFiles.value = [];
    selectedFile.value = null;
    
    // 同步更新所有文件和文件夹的isSelected状态
    if (currentFolders.value && currentFolders.value.length > 0) {
      currentFolders.value.forEach(folder => {
        if (folder.hasOwnProperty('isSelected')) {
          folder.isSelected = false;
        }
      });
    }
    if (currentFiles.value && currentFiles.value.length > 0) {
      currentFiles.value.forEach(file => {
        file.isSelected = false;
      });
    }
  } else {
    // 全选当前目录下的所有文件和文件夹
    const allItems = [...currentFolders.value, ...currentFiles.value];
    selectedFiles.value = [...allItems];

    // 同步更新所有文件和文件夹的isSelected状态
    if (currentFolders.value && currentFolders.value.length > 0) {
      currentFolders.value.forEach(folder => {
        if (folder.hasOwnProperty('isSelected')) {
          folder.isSelected = true;
        }
      });
    }
    if (currentFiles.value && currentFiles.value.length > 0) {
      currentFiles.value.forEach(file => {
        file.isSelected = true;
      });
    }

    if (allItems.length === 1) {
      selectedFile.value = allItems[0];
    } else if (allItems.length > 1) {
      selectedFile.value = {
        name: `${allItems.length} 个对象已选择`
      };
    }
  }

  isAllSelected.value = !isAllSelected.value;
};

// 更新全选状态
const updateSelectAllState = () => {
  const allItems = [...currentFolders.value, ...currentFiles.value];
  isAllSelected.value = selectedFiles.value.length === allItems.length && allItems.length > 0;
};

// 关闭侧边栏
const closeSidebar = () => {
  selectedFile.value = null;
};

// 处理删除操作
const handleDelete = async () => {
  if (!bucketName.value || selectedFiles.value.length === 0) {
    return;
  }

  try {
    // 构建删除请求数据
    const keysToDelete = selectedFiles.value.map(item => {
      // 对于文件夹，确保路径以/结尾
      if (item.isFolder && !item.fullPath.endsWith('/')) {
        return item.fullPath + '/';
      }
      return item.fullPath;
    });

    deleting.value = true;

    // 调用删除API
    const response = await delobject({
      bucket: bucketName.value,
      keys: keysToDelete
    });

    if (response.success === true || response.code === 0) {
      ElMessage.success(t('browser.deleteSuccess', {
        count: response.deleted || selectedFiles.value.length
      }));

      // 关闭侧边栏
      selectedFile.value = null;
      selectedFiles.value = [];
      isAllSelected.value = false;

      // 刷新文件列表
      await fetchObjects();
    } else {
      // 处理不同的错误情况
      if (response.message === 'NoSuchBucket' || response.message?.includes('NoSuchBucket')) {
        ElMessage.error(t('browser.bucketNotFound'));
      } else if (response.message === 'AccessDenied' || response.message?.includes('AccessDenied')) {
        ElMessage.error(t('common.accessDenied'));
      } else {
        ElMessage.error(response.message || t('browser.deleteFailed'));
      }
    }
  } catch (error) {
    console.error('Error deleting objects:', error);
    ElMessage.error(t('browser.deleteFailed'));
  } finally {
    deleting.value = false;
  }
};

// 处理下载操作
const handleDownload = async () => {
  if (!bucketName.value || selectedFiles.value.length === 0) {
    return;
  }

  try {
    // 构建下载请求数据
    const filePaths = selectedFiles.value.map(item => item.fullPath);
    
    // 确定文件名参数
    let filename = '';
    if (selectedFiles.value.length === 1) {
      // 单个文件下载时，使用文件名
      filename = selectedFiles.value[0].name;
    } else {
      // 多个文件下载时，创建一个压缩包名称
      const timestamp = new Date().toISOString().slice(0, 10).replace(/-/g, '');
      filename = `download-${timestamp}.zip`;
    }

    // 调用下载API
    const response = await getobject({
      bucket: bucketName.value,
      files: filePaths,
      filename: filename
    });
    
    // 下载API应该会自动触发文件下载，不需要额外处理
    
    // 如果需要处理特殊情况，可以在这里添加
    if (response?.success === false) {
      ElMessage.error(response.message || t('browser.downloadFailed'));
    }
  } catch (error) {
    console.error('Error downloading objects:', error);
    ElMessage.error(t('browser.downloadFailed'));
  }
};

// 返回桶列表
const backToList = () => {
  router.push('/buckets');
};

// 获取上一级目录的最后修改时间
const getParentDirLastModified = () => {
  // 简化实现，真实环境中可能需要从父目录获取
  return new Date().toISOString();
};

// 监听路径变化，重新获取数据
currentPath.value && fetchObjects();

// 获取文件扩展名
const getFileExtension = (fileName) => {
  const parts = fileName.split('.');
  if (parts.length > 1) {
    return parts.pop().toUpperCase();
  }
  return 'FILE';
};

// 根据文件类型获取图标类名
const getFileIconClass = (fileName) => {
  if (!fileName) return 'fas fa-file text-gray-500';

  const extension = getFileExtension(fileName).toLowerCase();

  if (['jpg', 'jpeg', 'png', 'gif', 'bmp', 'tiff'].includes(extension)) {
    return 'fas fa-file-image text-blue-500';
  } else if (['pdf'].includes(extension)) {
    return 'fas fa-file-pdf text-red-500';
  } else if (['doc', 'docx'].includes(extension)) {
    return 'fas fa-file-word text-blue-600';
  } else if (['xls', 'xlsx'].includes(extension)) {
    return 'fas fa-file-excel text-green-600';
  } else if (['ppt', 'pptx'].includes(extension)) {
    return 'fas fa-file-powerpoint text-orange-600';
  } else if (['mp4', 'avi', 'mov', 'wmv', 'flv'].includes(extension)) {
    return 'fas fa-file-video text-purple-600';
  } else if (['zip', 'rar', '7z', 'tar', 'gz'].includes(extension)) {
    return 'fas fa-file-archive text-yellow-600';
  } else if (['txt', 'md', 'log'].includes(extension)) {
    return 'fas fa-file-alt text-gray-600';
  } else if (['json', 'xml', 'html', 'css', 'js', 'php', 'py'].includes(extension)) {
    return 'fas fa-file-code text-indigo-600';
  } else {
    return 'fas fa-file text-gray-500';
  }
};

// 格式化日期
const formatDate = (dateString) => {
  try {
    const date = new Date(dateString);
    // 检查是否是有效的日期
    if (!isNaN(date.getTime())) {
      return date.toLocaleDateString('zh-CN', {
        year: 'numeric',
        month: '2-digit',
        day: '2-digit'
      }).replace(/\//g, '-');
    }
  } catch (error) {
    console.warn('Invalid date string:', dateString);
  }
  // 返回默认值或空字符串
  return '-';
};

// 格式化日期时间
const formatDateTime = (dateString) => {
  try {
    const date = new Date(dateString);
    // 检查是否是有效的日期
    if (!isNaN(date.getTime())) {
      return date.toLocaleDateString('zh-CN', {
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit'
      }).replace(/\//g, '-');
    }
  } catch (error) {
    console.warn('Invalid date string:', dateString);
  }
  // 返回默认值或空字符串
  return '-';
};

// 打开创建文件夹对话框
const openCreateFolderDialog = () => {
  folderName.value = '';
  createFolderError.value = '';
  createFolderDialogVisible.value = true;
};

// 关闭创建文件夹对话框
const closeCreateFolderDialog = () => {
  createFolderDialogVisible.value = false;
  folderName.value = '';
  createFolderError.value = '';
  creatingFolder.value = false;
};

// 处理创建文件夹
const handleCreateFolder = async () => {
  // 移除前后空格并去除前导斜杠（如果有）
  const trimmedName = folderName.value.trim().replace(/^\//, '');
  if (!trimmedName) {
    createFolderError.value = t('browser.folderNameRequired');
    return;
  }

  // 验证文件夹名称是否合法
  if (trimmedName.includes('/')) {
    createFolderError.value = t('browser.invalidFolderName');
    return;
  }

  // 构建完整的文件夹路径，确保不会有多余的斜杠
  let fullFolderPath = trimmedName + "/";
  if (currentPath.value) {
    // 去除currentPath末尾的斜杠（如果有），然后添加一个斜杠
    const normalizedPath = currentPath.value.endsWith('/')
      ? currentPath.value.slice(0, -1)
      : currentPath.value;
    fullFolderPath = `${normalizedPath}/${trimmedName}/`;
  }

  // 规范化路径，移除所有多余的斜杠（包括中间部分的）
  fullFolderPath = fullFolderPath.replace(/\/+/g, '/');

  try {
    creatingFolder.value = true;
    createFolderError.value = '';

    const response = await createfolder({
      bucket: bucketName.value,
      folder: fullFolderPath
    });
    console.log('Create folder response:', response);
    if (response.success === false) {
      if (response.message.includes('AlreadyExists')) {
        createFolderError.value = t('browser.folderAlreadyExists');
      } else if (response.message.includes('AccessDenied')) {
        createFolderError.value = t('browser.accessDenied');
      } else {
        createFolderError.value = response.message;
      }
      return;
    }

    // 成功创建文件夹后刷新列表
    closeCreateFolderDialog();
    await fetchObjects();
  } catch (error) {
    console.error('Error creating folder:', error);
    createFolderError.value = t('browser.createFolderFailed');
  } finally {
    creatingFolder.value = false;
  }
};

// 格式化文件大小
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

// 获取当前路径下的对象列表
const fetchObjects = async () => {
  if (!bucketName.value) return;

  loading.value = true;
  error.value = '';

  try {
    // 构造查询参数
    const params = {
      bucket: bucketName.value,
      prefix: currentPath.value ? currentPath.value + '/' : '',
      marker: '',
      delimiter: '/'
    };

    const result = await listobjects(params);

    if (result.success === true || result.code === 0) {
      // 确保正确处理嵌套的data对象
      objectsList.value = result.data.data || result.data;
    } else {
      // 处理不同的错误情况
      if (result.message === 'NoSuchBucket' || result.message?.includes('NoSuchBucket')) {
        error.value = t('browser.bucketNotFound');
        ElMessage.error(t('browser.bucketNotFound'));
      } else if (result.message === 'AccessDenied' || result.message?.includes('AccessDenied')) {
        error.value = t('common.accessDenied');
        ElMessage.error(t('common.accessDenied'));
      } else {
        error.value = result.message || t('browser.failedToLoadObjects');
        ElMessage.error(result.message || t('browser.failedToLoadObjects'));
      }
    }
  } catch (err) {
    console.error('Error fetching objects:', err);
    error.value = t('common.networkErrorPleaseRetry');
    ElMessage.error(t('common.networkErrorPleaseRetry'));
  } finally {
    loading.value = false;
    // 确保在DOM更新后调用，以便currentFiles计算属性已经更新
    setTimeout(() => {
      initializeFileSelectionState();
    }, 0);
  }
};

// 组件挂载时获取数据
onMounted(() => {
  fetchObjects();
});

// 使用系统的i18n翻译系统获取创建文件夹相关的翻译文本
</script>

<style scoped>
/* 主要容器样式 */
.browser-container {
  min-height: 100%;
  background: linear-gradient(135deg, #f5f7fa 0%, #e4e8f0 100%);
  padding: 1rem;
}

/* 头部导航样式 */
.header-nav {
  animation: fadeIn 0.5s ease-out forwards;
}

.nav-path {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
}

.back-btn {
  display: flex;
  align-items: center;
  justify-content: center;
  transition: all 0.3s ease;
}

.back-btn:hover {
  transform: translateX(-2px);
}

.breadcrumb {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
}

/* 上传按钮和创建文件夹按钮 */
.upload-btn,
.create-folder-btn {
  box-shadow: 0 4px 12px rgba(64, 158, 255, 0.2);
}

.upload-btn:disabled {
  opacity: 0.7;
  cursor: not-allowed;
  transform: none;
}

.upload-btn:hover,
.create-folder-btn:hover {
  box-shadow: 0 6px 16px rgba(64, 158, 255, 0.3);
}

.create-folder-btn {
  box-shadow: 0 4px 12px rgba(75, 85, 99, 0.2);
}

.create-folder-btn:hover {
  box-shadow: 0 6px 16px rgba(75, 85, 99, 0.3);
}

/* 对象列表容器 */
.objects-list-container {
  position: relative;
  overflow: hidden;
  animation: fadeIn 0.5s ease-out forwards;
  animation-delay: 0.1s;
}

/* 列表头部 */
.list-header {
  position: sticky;
  top: 0;
  z-index: 10;
}

/* 对象项 */
.object-item {
  transition: all 0.3s ease;
}

.object-item:hover {
  background-color: #f9fafb;
}

/* 空状态 */
.empty-state {
  animation: fadeIn 0.6s ease-out forwards;
}

/* 侧边栏覆盖层 */
.sidebar-overlay {
  opacity: 0;
}

.sidebar-overlay.show {
  opacity: 1;
}

/* 对象详情侧边栏 */
.object-sidebar {
  transform: translateX(100%);
  height: 100%;
}

.object-sidebar.show {
  transform: translateX(0);
}

.sidebar-content {
  height: calc(100% - 64px);
}

/* 文件预览 */
.file-preview {
  animation: fadeIn 0.5s ease-out forwards;
}

/* 操作按钮 */
.action-buttons {
  animation: fadeIn 0.5s ease-out forwards;
  animation-delay: 0.1s;
}

.download-btn {
  box-shadow: 0 4px 12px rgba(34, 197, 94, 0.2);
}

.download-btn:hover {
  box-shadow: 0 6px 16px rgba(34, 197, 94, 0.3);
}

.preview-btn {
  box-shadow: 0 4px 12px rgba(59, 130, 246, 0.2);
}

.preview-btn:hover {
  box-shadow: 0 6px 16px rgba(59, 130, 246, 0.3);
}

.delete-btn {
  box-shadow: 0 4px 12px rgba(239, 68, 68, 0.2);
}

.delete-btn:hover {
  box-shadow: 0 6px 16px rgba(239, 68, 68, 0.3);
}

/* 对象信息 */
.object-info {
  animation: fadeIn 0.5s ease-out forwards;
  animation-delay: 0.2s;
}

.info-grid {
  gap: 0.75rem;
}

.info-item {
  padding: 0.5rem 0;
}

/* 多对象选择状态样式 */
.multi-selection-state {
  animation: fadeIn 0.5s ease-out forwards;
}

.selection-icon {
  animation: pulse 1s ease-out;
}

@keyframes pulse {
  0% {
    transform: scale(0.8);
    opacity: 0;
  }

  50% {
    transform: scale(1.1);
  }

  100% {
    transform: scale(1);
    opacity: 1;
  }
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

.animate-fadeIn {
  animation: fadeIn 0.6s ease-out forwards;
  opacity: 0;
}

/* 响应式设计 */
@media (max-width: 768px) {
  .browser-container {
    padding: 0.75rem;
  }

  .header-nav {
    flex-direction: column;
    align-items: stretch;
    margin-bottom: 0.75rem;
    gap: 1rem;
  }

  .nav-path {
    flex-wrap: wrap;
    gap: 0.5rem;
  }

  .breadcrumb {
    min-width: 100%;
    margin-top: 0.25rem;
  }

  .header-actions {
    justify-content: center;
    flex-wrap: wrap;
  }

  .object-sidebar {
    width: 100%;
  }

  .action-buttons {
    flex-direction: column;
  }

  .action-buttons button {
    width: 100%;
  }
}

@media (max-width: 480px) {
  .browser-container {
    padding: 0.5rem;
  }

  .header-nav {
    margin-bottom: 0.5rem;
    gap: 0.75rem;
  }

  .header-nav h1 {
    font-size: 1.25rem;
  }

  .breadcrumb {
    font-size: 0.75rem;
  }

  .header-actions {
    gap: 0.5rem;
  }

  .upload-btn,
  .create-folder-btn {
    padding: 0.625rem 1rem !important;
    font-size: 0.8125rem !important;
    white-space: nowrap;
  }
}

@media (max-width: 380px) {
  .header-nav {
    margin-bottom: 0.5rem;
    gap: 0.5rem;
  }

  .header-actions {
    flex-direction: column;
  }

  .upload-btn,
  .create-folder-btn {
    width: 100%;
  }
}
</style>