<template>
  <div class="browser-container">
    <!-- 头部导航区域 -->
    <div class="header-nav flex flex-col sm:flex-row items-start sm:items-center justify-between mb-2 sm:mb-3 gap-1 sm:gap-2 transition-all duration-300">
      <div class="nav-path flex items-center">
        <button @click="backToList" class="back-btn p-2 text-gray-500 hover:text-blue-600 hover:bg-blue-50 rounded-lg transition-all duration-300 mr-2">
          <i class="fas fa-arrow-left"></i>
        </button>
        <h1 class="text-xl font-bold text-gray-800 transition-all duration-300 sm:text-2xl m-0">
          {{ bucketName }}
        </h1>
        <div class="breadcrumb ml-3 text-sm text-gray-500">
          <a href="#" class="hover:text-blue-600">存储桶</a>
          <span class="mx-1">/</span>
          <span class="text-gray-700">{{ bucketName }}</span>
          <span v-if="currentPath !== ''" class="mx-1">/</span>
          <span v-if="currentPath !== ''" class="text-gray-700 truncate ml-1">
            {{ currentPath }}
          </span>
        </div>
      </div>
      
      <div class="header-actions flex items-center gap-2">
        <button class="upload-btn bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded-lg font-medium flex items-center justify-center transition-all duration-300 transform hover:scale-105 hover:shadow-md whitespace-nowrap text-sm">
          <i class="fas fa-upload mr-1.5 text-sm"></i>
          上传文件
        </button>
        <button class="create-folder-btn bg-gray-600 hover:bg-gray-700 text-white px-4 py-2 rounded-lg font-medium flex items-center justify-center transition-all duration-300 transform hover:scale-105 hover:shadow-md whitespace-nowrap text-sm">
          <i class="fas fa-folder-plus mr-1.5 text-sm"></i>
          创建文件夹
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
            <input type="checkbox" :checked="isAllSelected" @change="toggleSelectAll" class="rounded text-blue-600 focus:ring-blue-500">
          </div>
          <div class="col-span-5 sm:col-span-3">名称</div>
          <div class="hidden sm:block col-span-2">类型</div>
          <div class="hidden lg:block col-span-2">大小</div>
          <div class="col-span-4 sm:col-span-4 lg:col-span-4">修改日期</div>
        </div>
      
      <!-- 对象列表 -->
      <div class="objects-list">
        <!-- 返回上一级 -->
        <div v-if="currentPath !== ''" @click="navigateUp" class="object-item flex items-center grid grid-cols-12 px-4 py-3 hover:bg-gray-50 transition-all duration-300 cursor-pointer animate-fadeIn">
          <div class="col-span-1 sm:col-span-1 flex items-center justify-center">
            <span class="text-gray-300">•</span>
          </div>
          <div class="col-span-5 sm:col-span-3 flex items-center">
            <i class="fas fa-folder-open text-gray-400 mr-3 text-lg"></i>
            <span class="font-medium text-gray-700">..</span>
          </div>
          <div class="hidden sm:flex col-span-2 items-center justify-start">
            <span class="text-gray-500">文件夹</span>
          </div>
          <div class="hidden lg:flex col-span-2 items-center justify-start">
            <span class="text-gray-500">-</span>
          </div>
          <div class="col-span-4 sm:col-span-4 lg:col-span-4 flex justify-end sm:justify-start sm:pl-4">
            <span class="text-gray-500 text-sm">{{ formatDate(getParentDirLastModified()) }}</span>
          </div>
        </div>
        
        <!-- 文件夹列表 -->
        <div v-for="folder in currentFolders" :key="folder.name" @click.stop="navigateToFolder(folder.name)" class="object-item flex items-center grid grid-cols-12 px-4 py-3 hover:bg-gray-50 transition-all duration-300 cursor-pointer animate-fadeIn" :style="{ animationDelay: (currentPath === '' ? 0 : 1) + (folder.index * 0.05) + 's' }">
          <div class="col-span-1 sm:col-span-1 flex items-center justify-center">
            <input type="checkbox" :checked="isSelected(folder)" @change.stop="toggleSelect(folder)" class="rounded text-blue-600 focus:ring-blue-500">
          </div>
          <div class="col-span-5 sm:col-span-3 flex items-center">
            <i class="fas fa-folder text-blue-500 mr-3 text-lg"></i>
            <span class="font-medium text-gray-700">{{ folder.name }}</span>
          </div>
          <div class="hidden sm:flex col-span-2 items-center justify-start">
            <span class="text-gray-500">文件夹</span>
          </div>
          <div class="hidden lg:flex col-span-2 items-center justify-start">
            <span class="text-gray-500">-</span>
          </div>
          <div class="col-span-4 sm:col-span-4 lg:col-span-4 flex justify-end sm:justify-start sm:pl-4">
            <span class="text-gray-500 text-sm">{{ formatDate(folder.lastModified) }}</span>
          </div>
        </div>
        
        <!-- 文件列表 -->
        <div v-for="file in currentFiles" :key="file.name" @click.stop="selectFile(file)" class="object-item flex items-center grid grid-cols-12 px-4 py-3 hover:bg-gray-50 transition-all duration-300 cursor-pointer animate-fadeIn" :style="{ animationDelay: (currentPath === '' ? 0 : 1) + (foldersCount + file.index) * 0.05 + 's' }">
          <div class="col-span-1 sm:col-span-1 flex items-center justify-center">
            <input type="checkbox" :checked="isSelected(file)" @change.stop="toggleSelect(file)" class="rounded text-blue-600 focus:ring-blue-500">
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
        
        <!-- 空状态 -->
        <div v-if="currentFolders.length === 0 && currentFiles.length === 0 && currentPath === ''" class="empty-state py-16 text-center">
          <div class="w-24 h-24 bg-gray-100 rounded-full flex items-center justify-center mx-auto mb-4">
            <svg class="w-12 h-12 text-gray-400" viewBox="0 0 256 256" fill="currentColor" xmlns="http://www.w3.org/2000/svg">
              <g>
                <path d="M244.1,8.4c-3.9-5.3-10.1-8.5-16.7-8.5H21.6C15,0,8.8,3.1,4.9,8.4C0.8,14-0.9,21,0.3,27.9c5.1,29.6,15.8,91.9,24.3,141.7v0.1C29,195,32.8,217.1,35,229.9c1.4,10.8,10.4,18.9,21.3,19.3h136.5c10.9-0.4,19.9-8.5,21.3-19.3l10.3-60.1l0.1-0.4L238.4,88v-0.2l10.3-59.9C249.9,21,248.3,14,244.1,8.4 M206.1,177h-163l-3.2-18.6h169.3L206.1,177z M220,95.3H28.9l-3.2-18.6h197.4L220,95.3z"/>
              </g>
            </svg>
          </div>
          <h3 class="text-xl font-semibold text-gray-800 mb-2">该存储桶为空</h3>
          <p class="text-gray-500 mb-6 max-w-md mx-auto">点击"上传文件"按钮开始向此存储桶添加内容，或创建文件夹组织您的文件。</p>
          <button class="upload-btn bg-blue-600 hover:bg-blue-700 text-white px-5 py-2.5 rounded-lg font-medium flex items-center justify-center transition-all duration-300 mx-auto transform hover:scale-105 hover:shadow-md">
            <i class="fas fa-upload mr-2"></i>
            上传第一个文件
          </button>
        </div>
      </div>
    </div>

      <!-- 对象详情侧边栏 -->
      <div v-if="selectedFile" class="object-sidebar w-full sm:w-96 bg-white shadow-lg transform transition-transform duration-300 ease-in-out show border-l border-gray-100">
        <div class="sidebar-header flex items-center justify-between p-4 border-b border-gray-200">
        <h2 class="text-lg font-semibold text-gray-800 truncate">文件详情</h2>
        <button class="close-btn p-2 text-gray-500 hover:text-gray-700 hover:bg-gray-100 rounded-lg transition-all duration-300" @click="closeSidebar">
          <i class="fas fa-times"></i>
        </button>
      </div>
      
      <div class="sidebar-content p-4 overflow-y-auto h-[calc(100%-64px)]">
        <!-- 多对象选择状态 -->
        <div v-if="selectedFile.isMultipleSelection" class="multi-selection-state text-center py-8">
          <div class="selection-icon w-20 h-20 bg-blue-100 rounded-full flex items-center justify-center mx-auto mb-4">
            <i class="fas fa-check-circle text-blue-600 text-4xl"></i>
          </div>
          <h3 class="font-semibold text-gray-800 text-lg mb-2">{{ selectedFiles.length }} 个对象已选择</h3>
          <p class="text-gray-500 text-sm mb-6">您可以对所选对象执行批量操作</p>
        </div>
        
        <!-- 单文件预览 -->
        <div v-else class="file-preview mb-6">
          <div class="preview-icon w-20 h-20 bg-gray-100 rounded-lg flex items-center justify-center mx-auto mb-2">
            <i :class="getFileIconClass(selectedFile.name)" class="text-4xl"></i>
          </div>
          <div class="text-center">
            <h3 class="font-medium text-gray-800 truncate">{{ selectedFile.name }}</h3>
            <p class="text-gray-500 text-sm">{{ formatSize(selectedFile.size) }}</p>
          </div>
        </div>
        
        <!-- 操作按钮 -->
        <div class="action-buttons flex justify-center flex-wrap gap-2 mb-6">
          <button class="download-btn flex-1 sm:flex-none bg-green-600 hover:bg-green-700 text-white px-4 py-2.25 rounded-lg font-medium flex items-center justify-center transition-all duration-300 transform hover:scale-105 hover:shadow-md whitespace-nowrap text-sm">
            <i class="fas fa-download mr-1.5 text-sm"></i>
            下载
          </button>
          <button v-if="selectedFiles.length <= 1" class="preview-btn flex-1 sm:flex-none bg-blue-600 hover:bg-blue-700 text-white px-4 py-2.25 rounded-lg font-medium flex items-center justify-center transition-all duration-300 transform hover:scale-105 hover:shadow-md whitespace-nowrap text-sm">
            <i class="fas fa-eye mr-1.5 text-sm"></i>
            预览
          </button>
          <button class="delete-btn flex-1 sm:flex-none bg-red-600 hover:bg-red-700 text-white px-4 py-2.25 rounded-lg font-medium flex items-center justify-center transition-all duration-300 transform hover:scale-105 hover:shadow-md whitespace-nowrap text-sm">
            <i class="fas fa-trash-alt mr-1.5 text-sm"></i>
            删除
          </button>
        </div>
        
        <!-- 单对象信息 -->
        <div v-if="!selectedFile.isMultipleSelection" class="object-info">
          <h4 class="text-sm font-semibold text-gray-500 uppercase mb-3">Object Info</h4>
          <div class="info-grid grid grid-cols-1 gap-3">
            <div class="info-item">
              <label class="text-xs text-gray-500 block mb-1">名称</label>
              <p class="text-gray-800 font-medium truncate">{{ selectedFile.name }}</p>
            </div>
            <div class="info-item">
              <label class="text-xs text-gray-500 block mb-1">大小</label>
              <p class="text-gray-800">{{ formatSize(selectedFile.size) }} ({{ selectedFile.size }} bytes)</p>
            </div>
            <div class="info-item">
              <label class="text-xs text-gray-500 block mb-1">创建时间</label>
              <p class="text-gray-800">{{ formatDateTime(selectedFile.createdAt) }}</p>
            </div>
            <div class="info-item">
              <label class="text-xs text-gray-500 block mb-1">最后修改时间</label>
              <p class="text-gray-800">{{ formatDateTime(selectedFile.lastModified) }}</p>
            </div>
            <div class="info-item">
              <label class="text-xs text-gray-500 block mb-1">ETag</label>
              <p class="text-gray-800 text-xs break-all">{{ selectedFile.etag }}</p>
            </div>
            <div class="info-item" v-if="selectedFile.tags && selectedFile.tags.length > 0">
              <label class="text-xs text-gray-500 block mb-1">标签</label>
              <div class="tags flex flex-wrap gap-1">
                <span v-for="tag in selectedFile.tags" :key="tag.key" class="inline-block bg-blue-50 text-blue-600 px-2 py-0.5 rounded text-xs font-medium">
                  {{ tag.key }}: {{ tag.value }}
                </span>
              </div>
            </div>
            <div class="info-item">
              <label class="text-xs text-gray-500 block mb-1">块数量</label>
              <p class="text-gray-800">{{ selectedFile.chunks || 1 }}</p>
            </div>
            <div class="info-item">
              <label class="text-xs text-gray-500 block mb-1">存储路径</label>
              <p class="text-gray-800 text-xs truncate">{{ bucketName }}/{{ currentPath ? currentPath + '/' : '' }}{{ selectedFile.name }}</p>
            </div>
          </div>
        </div>
      </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed } from 'vue';
import { useRoute, useRouter } from 'vue-router';

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

// 模拟数据 - 桶内对象
const bucketObjects = ref({
  '': {
    folders: [
      { name: 'documents', lastModified: '2023-05-15T10:30:00Z', index: 0 },
      { name: 'images', lastModified: '2023-04-22T14:15:00Z', index: 1 },
      { name: 'videos', lastModified: '2023-06-01T09:45:00Z', index: 2 },
      { name: 'backup', lastModified: '2023-03-10T16:20:00Z', index: 3 }
    ],
    files: [
      { 
        name: 'README.md', 
        size: 12345, 
        lastModified: '2023-05-28T11:05:00Z', 
        createdAt: '2023-05-28T11:05:00Z',
        etag: '"abcdef1234567890abcdef1234567890"',
        tags: [{ key: 'type', value: 'document' }],
        chunks: 1,
        index: 0
      },
      { 
        name: 'config.json', 
        size: 5678, 
        lastModified: '2023-02-18T15:30:00Z', 
        createdAt: '2023-02-18T15:30:00Z',
        etag: '"1234567890abcdef1234567890abcdef"',
        tags: [{ key: 'environment', value: 'production' }],
        chunks: 1,
        index: 1
      },
      { 
        name: 'data.csv', 
        size: 1048576, 
        lastModified: '2023-06-10T08:15:00Z', 
        createdAt: '2023-06-10T08:15:00Z',
        etag: '"90abcdef1234567890abcdef12345678"',
        tags: [{ key: 'source', value: 'analytics' }],
        chunks: 2,
        index: 2
      }
    ]
  },
  'documents': {
    folders: [],
    files: [
      { 
        name: 'report.pdf', 
        size: 2097152, 
        lastModified: '2023-05-01T10:30:00Z', 
        createdAt: '2023-05-01T10:30:00Z',
        etag: '"abcdef9012345678abcdef9012345678"',
        tags: [{ key: 'type', value: 'report' }, { key: 'quarter', value: 'Q2' }],
        chunks: 3,
        index: 0
      },
      { 
        name: 'presentation.pptx', 
        size: 5242880, 
        lastModified: '2023-04-15T14:20:00Z', 
        createdAt: '2023-04-15T14:20:00Z',
        etag: '"9012345678abcdef9012345678abcdef"',
        tags: [{ key: 'type', value: 'presentation' }],
        chunks: 6,
        index: 1
      }
    ]
  },
  'images': {
    folders: [
      { name: 'logos', lastModified: '2023-03-20T09:15:00Z', index: 0 },
      { name: 'screenshots', lastModified: '2023-05-10T16:40:00Z', index: 1 }
    ],
    files: [
      { 
        name: 'banner.jpg', 
        size: 1572864, 
        lastModified: '2023-06-05T11:25:00Z', 
        createdAt: '2023-06-05T11:25:00Z',
        etag: '"def1234567890abcdef1234567890ab"',
        tags: [{ key: 'type', value: 'image' }, { key: 'format', value: 'jpg' }],
        chunks: 2,
        index: 0
      }
    ]
  },
  'videos': {
    folders: [],
    files: [
      { 
        name: 'demo.mp4', 
        size: 268435456, 
        lastModified: '2023-05-20T15:30:00Z', 
        createdAt: '2023-05-20T15:30:00Z',
        etag: '"7890abcdef1234567890abcdef12345"',
        tags: [{ key: 'type', value: 'video' }, { key: 'format', value: 'mp4' }],
        chunks: 150,
        index: 0
      }
    ]
  },
  'backup': {
    folders: [],
    files: [
      { 
        name: 'database-backup-20230601.sql.gz', 
        size: 1073741824, 
        lastModified: '2023-06-01T00:00:00Z', 
        createdAt: '2023-06-01T00:00:00Z',
        etag: '"1234567890abcdef1234567890abcdef"',
        tags: [{ key: 'type', value: 'backup' }, { key: 'date', value: '20230601' }],
        chunks: 600,
        index: 0
      }
    ]
  },
  'images/logos': {
    folders: [],
    files: [
      { 
        name: 'company-logo.png', 
        size: 524288, 
        lastModified: '2023-03-20T09:15:00Z', 
        createdAt: '2023-03-20T09:15:00Z',
        etag: '"abcdef1234567890abcdef12345678"',
        tags: [{ key: 'type', value: 'logo' }, { key: 'format', value: 'png' }],
        chunks: 1,
        index: 0
      }
    ]
  },
  'images/screenshots': {
    folders: [],
    files: [
      { 
        name: 'dashboard.png', 
        size: 2097152, 
        lastModified: '2023-05-10T16:40:00Z', 
        createdAt: '2023-05-10T16:40:00Z',
        etag: '"90abcdef1234567890abcdef12345678"',
        tags: [{ key: 'type', value: 'screenshot' }, { key: 'format', value: 'png' }],
        chunks: 2,
        index: 0
      }
    ]
  }
});

// 当前目录下的文件夹
const currentFolders = computed(() => {
  return bucketObjects.value[currentPath.value]?.folders || [];
});

// 当前目录下的文件
const currentFiles = computed(() => {
  return bucketObjects.value[currentPath.value]?.files || [];
});

// 文件夹数量
const foldersCount = computed(() => {
  return currentFolders.value.length;
});

// 返回上一级
const navigateUp = () => {
  if (currentPath.value === '') return;
  
  const pathParts = currentPath.value.split('/');
  pathParts.pop();
  currentPath.value = pathParts.join('/');
};

// 导航到子文件夹
const navigateToFolder = (folderName) => {
  if (currentPath.value === '') {
    currentPath.value = folderName;
  } else {
    currentPath.value = `${currentPath.value}/${folderName}`;
  }
};

// 选择单个文件
const selectFile = (file) => {
  selectedFile.value = file;
  selectedFiles.value = [file];
};

// 切换选择状态
const toggleSelect = (item) => {
  const index = selectedFiles.value.findIndex(f => f.name === item.name);
  if (index > -1) {
    selectedFiles.value.splice(index, 1);
  } else {
    selectedFiles.value.push(item);
  }
  
  // 如果只选择了一个文件，显示其详情
  if (selectedFiles.value.length === 1) {
    selectedFile.value = selectedFiles.value[0];
  } else if (selectedFiles.value.length > 1) {
    // 选择多个文件时，显示自定义标题
    selectedFile.value = {
      name: `${selectedFiles.value.length} 个对象已选择`,
      isMultipleSelection: true
    };
  } else {
    selectedFile.value = null;
  }
  
  // 更新全选状态
  updateSelectAllState();
};

// 检查是否被选中
const isSelected = (item) => {
  return selectedFiles.value.some(f => f.name === item.name);
};

// 切换全选
const toggleSelectAll = () => {
  if (isAllSelected.value) {
    // 取消全选
    selectedFiles.value = [];
    selectedFile.value = null;
  } else {
    // 全选当前目录下的所有文件和文件夹
    const allItems = [...currentFolders.value, ...currentFiles.value];
    selectedFiles.value = [...allItems];
    
    if (allItems.length === 1) {
      selectedFile.value = allItems[0];
    } else if (allItems.length > 1) {
      selectedFile.value = {
        name: `${allItems.length} 个对象已选择`,
        isMultipleSelection: true
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

// 返回桶列表
const backToList = () => {
  router.push('/buckets');
};

// 获取上一级目录的最后修改时间
const getParentDirLastModified = () => {
  if (currentPath.value === '') return new Date().toISOString();
  
  const parentPath = currentPath.value.split('/').slice(0, -1).join('/');
  const parentObjects = bucketObjects.value[parentPath];
  
  if (parentObjects) {
    const allObjects = [...parentObjects.folders, ...parentObjects.files];
    if (allObjects.length > 0) {
      return allObjects.sort((a, b) => new Date(b.lastModified) - new Date(a.lastModified))[0].lastModified;
    }
  }
  
  return new Date().toISOString();
};

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
  const date = new Date(dateString);
  return date.toLocaleDateString('zh-CN', {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit'
  }).replace(/\//g, '-');
};

// 格式化日期时间
const formatDateTime = (dateString) => {
  const date = new Date(dateString);
  return date.toLocaleDateString('zh-CN', {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit'
  }).replace(/\//g, '-');
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