<template>
  <div class="browser-container">
    <!-- 头部导航区域 -->
    <div class="header-nav flex flex-col sm:flex-row items-start sm:items-center justify-between mb-2 sm:mb-3 gap-1 sm:gap-2 transition-all duration-300">
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
          @click="handleUploadClick" :disabled="uploading">
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
        <div class="list-header grid grid-cols-12 px-4 py-3 border-b border-gray-200 bg-gray-50 text-sm font-medium text-gray-500">
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
          <div v-for="folder in currentFolders" :key="folder.fullPath"
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
          <div v-for="file in currentFiles" :key="file.fullPath" @click.stop="selectFile(file)"
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
            <div class="w-12 h-12 border-4 border-blue-200 border-t-blue-600 rounded-full animate-spin mx-auto mb-4"></div>
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
            <p class="text-gray-500 mb-6 max-w-md mx-auto">{{ currentPath === '' ? t('browser.emptyBucketHint') : t('browser.emptyFolderHint') }}</p>
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
            <div class="selection-icon w-20 h-20 bg-blue-100 rounded-full flex items-center justify-center mx-auto mb-4">
              <i class="fas fa-check-circle text-blue-600 text-4xl"></i>
            </div>
            <h3 class="font-semibold text-gray-800 text-lg mb-2">{{ t('browser.selectedObjectsCount', { count: selectedFiles.length }) }}</h3>
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
                <p class="text-gray-800">{{ selectedFile.isFolder ? '-' : formatSize(selectedFile.size) + ' (' + selectedFile.size + ' bytes)' }}</p>
              </div>
              <div class="info-item">
                <label class="text-xs text-gray-500 block mb-1">{{ t('common.createdAt') }}</label>
                <p class="text-gray-800">{{ selectedFile.isFolder ? '-' : formatDateTime(selectedFile.createdAt) }}</p>
              </div>
              <div class="info-item">
                <label class="text-xs text-gray-500 block mb-1">{{ t('common.lastModified') }}</label>
                <p class="text-gray-800">{{ selectedFile.isFolder ? '-' : formatDateTime(selectedFile.lastModified) }}</p>
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
                <p class="text-gray-800 text-xs truncate">{{ bucketName }}/{{ currentPath ? currentPath + '/' : '' }}{{ selectedFile.name }}</p>
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
            {{ t('browser.createFolderHint', { path: currentPath === '' ? bucketName : `${bucketName}/${currentPath}` }) }}
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

// ====== 变量定义 ======
const { t } = useI18n();
const route = useRoute();
const router = useRouter();

// 路由参数
const bucketName = ref(route.params.name || '');

// 路径和文件状态
const currentPath = ref('');
const objectsList = ref([]);

// 选中状态
const selectedFile = ref(null);
const selectedFiles = ref([]);
const isAllSelected = ref(false);

// 创建文件夹状态
const createFolderDialogVisible = ref(false);
const folderName = ref('');
const createFolderError = ref('');
const creatingFolder = ref(false);

// 上传状态
const uploading = ref(false);
const uploadProgress = ref(0);
const uploadError = ref('');

// 加载和错误状态
const loading = ref(false);
const error = ref('');

// 删除状态
const deleting = ref(false);

// 分页状态
const nextMarker = ref('');

// 文件输入引用
const fileInput = ref(null);

// ====== 计算属性 ======
const currentFolders = computed(() => {
  const data = processObjectsData(objectsList.value);
  return data.folders;
});

const currentFiles = computed(() => {
  const data = processObjectsData(objectsList.value);
  return data.files;
});

const foldersCount = computed(() => currentFolders.value.length);

const isMultipleSelection = computed(() => selectedFiles.value.length > 1);

// ====== 核心数据处理函数 ======
const processObjectsData = (data) => {
  const folders = [];
  const files = [];

  if (data?.objects) {
    data.objects.forEach((item, idx) => {
      if (item.isFolder) {
        const folderName = item.name.split('/').filter(p => p).pop();
        folders.push({
          name: folderName,
          fullPath: item.name,
          originalItem: item,
          lastModified: item.lastModify ? item.lastModify : new Date().toISOString(),
          index: idx,
          isFolder: true
        });
      } else {
        const fileName = item.name.split('/').pop();
        
        let tags = [];
        if (item.tags && typeof item.tags === 'object') {
          tags = Object.entries(item.tags).map(([key, value]) => ({ key, value }));
        }

        files.push({
          name: fileName,
          fullPath: item.name,
          originalItem: item,
          size: item.size || 0,
          lastModified: item.lastModify ? item.lastModify : new Date().toISOString(),
          createdAt: item.createdAt ? item.createdAt : new Date().toISOString(),
          etag: item.etag || '',
          tags: tags,
          chunks: item.chunks ? item.chunks.length : 1,
          index: idx,
          isSelected: false
        });
      }
    });
  }

  nextMarker.value = data?.nextMarker || '';
  return { folders, files };
};

// ====== 路径导航函数 ======
const navigateUp = () => {
  if (currentPath.value === '') return;
  
  const pathParts = currentPath.value.split('/');
  pathParts.pop();
  currentPath.value = pathParts.join('/');
  fetchObjects();
};

const navigateToFolder = (folder) => {
  currentPath.value = folder.fullPath;
  selectedFile.value = null;
  fetchObjects();
};

const backToList = () => {
  router.push('/buckets');
};

const getParentDirLastModified = () => {
  return new Date().toISOString();
};

// ====== 文件选择与操作函数 ======
const selectFile = (file) => {
  selectedFile.value = file;
  
  if (file.hasOwnProperty('isSelected')) {
    file.isSelected = !file.isSelected;
    const index = selectedFiles.value.findIndex(f => f.fullPath === file.fullPath);
    if (file.isSelected && index === -1) {
      selectedFiles.value.push(file);
    } else if (!file.isSelected && index > -1) {
      selectedFiles.value.splice(index, 1);
    }
  }
};

const toggleSelect = (item) => {
  const index = selectedFiles.value.findIndex(f => f.fullPath === item.fullPath);
  if (index > -1) {
    selectedFiles.value.splice(index, 1);
    if (item.hasOwnProperty('isSelected')) {
      item.isSelected = false;
    }
  } else {
    selectedFiles.value.push(item);
    if (item.hasOwnProperty('isSelected')) {
      item.isSelected = true;
    }
  }

  if (selectedFiles.value.length === 1) {
    selectedFile.value = selectedFiles.value[0];
  } else if (selectedFiles.value.length > 1) {
    selectedFile.value = {
      name: `${selectedFiles.value.length} 个对象已选择`
    };
  } else {
    selectedFile.value = null;
  }

  updateSelectAllState();
};

const isSelected = (item) => {
  return selectedFiles.value.some(f => f.fullPath === item.fullPath);
};

const toggleSelectAll = () => {
  if (isAllSelected.value) {
    selectedFiles.value = [];
    selectedFile.value = null;
    
    currentFolders.value.forEach(folder => {
      if (folder.hasOwnProperty('isSelected')) folder.isSelected = false;
    });
    currentFiles.value.forEach(file => file.isSelected = false);
  } else {
    const allItems = [...currentFolders.value, ...currentFiles.value];
    selectedFiles.value = [...allItems];

    currentFolders.value.forEach(folder => {
      if (folder.hasOwnProperty('isSelected')) folder.isSelected = true;
    });
    currentFiles.value.forEach(file => file.isSelected = true);
    
    if (allItems.length === 1) {
      selectedFile.value = allItems[0];
    } else if (allItems.length > 1) {
      selectedFile.value = { name: `${allItems.length} 个对象已选择` };
    }
  }

  isAllSelected.value = !isAllSelected.value;
};

const updateSelectAllState = () => {
  const allItems = [...currentFolders.value, ...currentFiles.value];
  isAllSelected.value = selectedFiles.value.length === allItems.length && allItems.length > 0;
};

// ====== 上传功能函数 ======
const handleUploadClick = () => {
  fileInput.value?.click();
};

const handleFileSelect = async (event) => {
  const files = event.target.files;
  if (!files || files.length === 0) return;

  for (let i = 0; i < files.length; i++) {
    await uploadFile(files[i]);
  }

  event.target.value = '';
};

const uploadFile = async (file) => {
  if (!bucketName.value) {
    ElMessage.error(t('browser.noBucketSelected'));
    return;
  }

  uploading.value = true;
  uploadError.value = '';
  uploadProgress.value = 0;

  try {
    const objectName = currentPath.value ? `${currentPath.value}/${file.name}` : file.name;
    const formData = new FormData();
    formData.append('file', file);
    formData.append('bucket', bucketName.value);
    formData.append('object', objectName);
    formData.append('contentType', file.type || 'application/octet-stream');

    const result = await putobject(formData, {
      onUploadProgress: (progressEvent) => {
        if (progressEvent.total) {
          uploadProgress.value = Math.round((progressEvent.loaded * 100) / progressEvent.total);
        }
      }
    });

    if (result.success === true || result.code === 0) {
      ElMessage.success(t('browser.uploadSuccess', { fileName: file.name }));
      await fetchObjects();
    } else {
      handleUploadError(result);
    }
  } catch (error) {
    console.error('Error uploading file:', error);
    ElMessage.error(t('browser.uploadFailed'));
  } finally {
    uploading.value = false;
    uploadProgress.value = 0;
  }
};

const handleUploadError = (result) => {
  if (result.message === 'NoSuchBucket' || result.message?.includes('NoSuchBucket')) {
    ElMessage.error(t('browser.bucketNotFound'));
  } else if (result.message === 'AccessDenied' || result.message?.includes('AccessDenied')) {
    ElMessage.error(t('common.accessDenied'));
  } else {
    ElMessage.error(result.message || t('browser.uploadFailed'));
  }
};

// ====== 创建文件夹函数 ======
const openCreateFolderDialog = () => {
  folderName.value = '';
  createFolderError.value = '';
  createFolderDialogVisible.value = true;
};

const closeCreateFolderDialog = () => {
  createFolderDialogVisible.value = false;
  folderName.value = '';
  createFolderError.value = '';
  creatingFolder.value = false;
};

const handleCreateFolder = async () => {
  const trimmedName = folderName.value.trim().replace(/^\//, '');
  if (!trimmedName) {
    createFolderError.value = t('browser.folderNameRequired');
    return;
  }

  if (trimmedName.includes('/')) {
    createFolderError.value = t('browser.invalidFolderName');
    return;
  }

  let fullFolderPath = trimmedName + "/";
  if (currentPath.value) {
    const normalizedPath = currentPath.value.endsWith('/')
      ? currentPath.value.slice(0, -1)
      : currentPath.value;
    fullFolderPath = `${normalizedPath}/${trimmedName}/`;
  }

  fullFolderPath = fullFolderPath.replace(/\/+/g, '/');

  try {
    creatingFolder.value = true;
    createFolderError.value = '';

    const response = await createfolder({
      bucket: bucketName.value,
      folder: fullFolderPath
    });

    if (response.success === false) {
      if (response.msg.includes('AlreadyExists')) {
        createFolderError.value = t('browser.folderAlreadyExists');
      } else if (response.msg.includes('AccessDenied')) {
        createFolderError.value = t('browser.accessDenied');
      } else {
        createFolderError.value = response.msg;
      }
      return;
    }

    closeCreateFolderDialog();
    await fetchObjects();
  } catch (error) {
    console.error('Error creating folder:', error);
    createFolderError.value = t('browser.createFolderFailed');
  } finally {
    creatingFolder.value = false;
  }
};

// ====== 文件管理操作函数 ======
const handleDelete = async () => {
  if (!bucketName.value || selectedFiles.value.length === 0) return;

  try {
    const keysToDelete = selectedFiles.value.map(item => {
      if (item.isFolder && !item.fullPath.endsWith('/')) {
        return item.fullPath + '/';
      }
      return item.fullPath;
    });

    deleting.value = true;

    const response = await delobject({
      bucket: bucketName.value,
      keys: keysToDelete
    });

    if (response.success === true || response.code === 0) {
      ElMessage.success(t('browser.deleteSuccess', {
        count: response.deleted || selectedFiles.value.length
      }));

      selectedFile.value = null;
      selectedFiles.value = [];
      isAllSelected.value = false;

      await fetchObjects();
    } else {
      handleDeleteError(response);
    }
  } catch (error) {
    console.error('Error deleting objects:', error);
    ElMessage.error(t('browser.deleteFailed'));
  } finally {
    deleting.value = false;
  }
};

const handleDeleteError = (response) => {
  if (response.msg === 'NoSuchBucket' || response.msg?.includes('NoSuchBucket')) {
    ElMessage.error(t('browser.bucketNotFound'));
  } else if (response.msg === 'AccessDenied' || response.msg?.includes('AccessDenied')) {
    ElMessage.error(t('common.accessDenied'));
  } else {
    ElMessage.error(response.msg || t('browser.deleteFailed'));
  }
};

const handleDownload = async () => {
  if (!bucketName.value || selectedFiles.value.length === 0) return;

  try {
    const filePaths = selectedFiles.value.map(item => item.fullPath);
    
    let filename = '';
    if (selectedFiles.value.length === 1) {
      filename = selectedFiles.value[0].name;
    } else {
      const timestamp = new Date().toISOString().slice(0, 10).replace(/-/g, '');
      filename = `download-${timestamp}.zip`;
    }

    const response = await getobject({
      bucket: bucketName.value,
      files: filePaths,
      filename: filename
    });
    
    if (response?.success === false) {
      ElMessage.error(response.msg || t('browser.downloadFailed'));
    }
  } catch (error) {
    console.error('Error downloading objects:', error);
    ElMessage.error(t('browser.downloadFailed'));
  }
};

// ====== 侧边栏控制函数 ======
const closeSidebar = () => {
  selectedFile.value = null;
};

// ====== 数据获取函数 ======
const fetchObjects = async () => {
  if (!bucketName.value) return;

  loading.value = true;
  error.value = '';

  try {
    const params = {
      bucket: bucketName.value,
      prefix: currentPath.value ? currentPath.value + '/' : '',
      marker: '',
      delimiter: '/'
    };

    const result = await listobjects(params);

    if (result.success === true || result.code === 0) {
      objectsList.value = result.data.data || result.data;
    } else {
      handleError(result);
    }
  } catch (err) {
    console.error('Error fetching objects:', err);
    error.value = t('common.networkErrorPleaseRetry');
    ElMessage.error(t('common.networkErrorPleaseRetry'));
  } finally {
    loading.value = false;
  }
};

const handleError = (result) => {
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
};

// ====== 工具函数 ======
const getFileExtension = (fileName) => {
  const parts = fileName.split('.');
  if (parts.length > 1) {
    return parts.pop().toUpperCase();
  }
  return 'FILE';
};

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

const formatDate = (dateString) => {
  try {
    const date = new Date(dateString);
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
  return '-';
};

const formatDateTime = (dateString) => {
  try {
    const date = new Date(dateString);
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
  return '-';
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

// ====== 组件生命周期 ======
onMounted(() => {
  fetchObjects();
});

// 监听currentFiles变化，初始化选择状态
watch(() => currentFiles.value, (newFiles) => {
  if (newFiles && newFiles.length > 0) {
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
