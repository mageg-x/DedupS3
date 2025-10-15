<template>
  <div class="dashboard-container">
    <!-- 统计卡片区域 -->
    <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-8">
      <!-- 使用v-for循环渲染所有卡片 -->
      <div 
        v-for="(card, index) in statCards" 
        :key="card.id"
        class="stat-card bg-white rounded-2xl shadow-card border border-gray-100 hover:shadow-card-hover p-6 transition-all duration-500 relative animate-fadeIn"
      >
        <!-- 右上角固定图标 -->
        <div :class="card.iconContainerClass" class="absolute top-4 right-4">
          <div :class="card.iconWrapperClass">
            <i :class="card.iconClass" class="text-white text-lg"></i>
          </div>
        </div>
        
        <!-- 主要内容 -->
        <div class="mr-8">
          <p class="text-gray-500 text-sm font-medium mb-2">{{ card.title }}</p>
          <h3 class="text-3xl font-bold text-gray-800 mt-4">{{ card.value }}</h3>
        </div>
        
        <!-- 底部信息 -->
        <div class="mt-5 flex items-center text-sm">
          <span 
            v-if="card.footerType === 'growth'"
            class="text-green-500 font-medium flex items-center bg-green-50 py-1 px-2 rounded-lg"
          >
            <i class="fas fa-arrow-up w-3 h-3 mr-1.5"></i>
            +{{ card.growthValue }}%
            <span class="text-gray-400 ml-2 whitespace-nowrap">{{ t('dashboard.comparedToLastMonth') }}</span>
          </span>
          <span 
            v-else-if="card.footerType === 'saving'"
            class="text-green-500 font-medium flex items-center bg-green-50 py-1 px-2 rounded-lg"
          >
            <i class="fas fa-arrow-down w-3 h-3 mr-1.5"></i>
            {{ card.savingText }}
          </span>
          <span 
            v-else-if="card.footerType === 'reuse'"
            class="text-green-500 font-medium flex items-center bg-green-50 py-1 px-2 rounded-lg"
          >
            <i class="fas fa-check w-3 h-3 mr-1.5"></i>
            {{ card.reuseText }}
          </span>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue';
import { useI18n } from 'vue-i18n';
import { getstats } from '../api/admin';

// 国际化
const { t } = useI18n();

// 统计数据状态
const stats = ref({
  bucketCount: 0,
  objectCount: 0,
  blockCount: 0,
  chunkCount: 0,
  originalSize: 0,
  actualSize: 0,
  compressionRatio: 0,
  savedSize: 0,
  bucketGrowth: 0,
  objectGrowth: 0,
  blockGrowth: 0,
  chunkGrowth: 0,
  sizeGrowth: 0,
  actualSizeGrowth: 0,
  reusedChunkCount: 0,
  reusedSize: 0,
  reuseRatio: 0
});

// 格式化函数
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

// 计算属性 - 卡片配置
const statCards = computed(() => [
  {
    id: 'bucket-count',
    title: t('dashboard.bucketCount'),
    value: stats.value.bucketCount,
    iconContainerClass: 'w-10 h-10 rounded-2xl bg-gradient-to-br from-blue-50 to-blue-100 flex items-center justify-center text-blue-500',
    iconWrapperClass: 'w-8 h-8 bg-gradient-to-br from-blue-500 to-blue-600 rounded-xl flex items-center justify-center shadow-blue',
    iconClass: 'fas fa-archive',
    footerType: 'growth',
    growthValue: stats.value.bucketGrowth
  },
  {
    id: 'object-count',
    title: t('dashboard.objectCount'),
    value: formatNumber(stats.value.objectCount),
    iconContainerClass: 'w-10 h-10 rounded-2xl bg-gradient-to-br from-purple-50 to-purple-100 flex items-center justify-center text-purple-500',
    iconWrapperClass: 'w-8 h-8 bg-gradient-to-br from-purple-500 to-purple-600 rounded-xl flex items-center justify-center shadow-purple',
    iconClass: 'fas fa-file',
    footerType: 'growth',
    growthValue: stats.value.objectGrowth
  },
  {
    id: 'block-count',
    title: t('dashboard.blockCount'),
    value: formatNumber(stats.value.blockCount),
    iconContainerClass: 'w-10 h-10 rounded-2xl bg-gradient-to-br from-green-50 to-green-100 flex items-center justify-center text-green-500',
    iconWrapperClass: 'w-8 h-8 bg-gradient-to-br from-green-500 to-green-600 rounded-xl flex items-center justify-center shadow-green',
    iconClass: 'fas fa-box',
    footerType: 'growth',
    growthValue: stats.value.blockGrowth
  },
  {
    id: 'chunk-count',
    title: t('dashboard.chunkCount'),
    value: formatNumber(stats.value.chunkCount),
    iconContainerClass: 'w-10 h-10 rounded-2xl bg-gradient-to-br from-amber-50 to-amber-100 flex items-center justify-center text-amber-500',
    iconWrapperClass: 'w-8 h-8 bg-gradient-to-br from-amber-500 to-amber-600 rounded-xl flex items-center justify-center shadow-amber',
    iconClass: 'fas fa-chart-pie',
    footerType: 'growth',
    growthValue: stats.value.chunkGrowth
  },
  {
    id: 'original-size',
    title: t('dashboard.originalSize'),
    value: formatSize(stats.value.originalSize),
    iconContainerClass: 'w-10 h-10 rounded-2xl bg-gradient-to-br from-red-50 to-red-100 flex items-center justify-center text-red-500',
    iconWrapperClass: 'w-8 h-8 bg-gradient-to-br from-red-500 to-red-600 rounded-xl flex items-center justify-center shadow-red',
    iconClass: 'fas fa-database',
    footerType: 'growth',
    growthValue: stats.value.sizeGrowth
  },
  {
    id: 'actual-size',
    title: t('dashboard.actualSize'),
    value: formatSize(stats.value.actualSize),
    iconContainerClass: 'w-10 h-10 rounded-2xl bg-gradient-to-br from-cyan-50 to-cyan-100 flex items-center justify-center text-cyan-500',
    iconWrapperClass: 'w-8 h-8 bg-gradient-to-br from-cyan-500 to-cyan-600 rounded-xl flex items-center justify-center shadow-cyan',
    iconClass: 'fas fa-server',
    footerType: 'growth',
    growthValue: stats.value.actualSizeGrowth
  },
  {
    id: 'compression-ratio',
    title: t('dashboard.compressionRatio'),
    value: stats.value.compressionRatio + 'x',
    iconContainerClass: 'w-10 h-10 rounded-2xl bg-gradient-to-br from-indigo-50 to-indigo-100 flex items-center justify-center text-indigo-500',
    iconWrapperClass: 'w-8 h-8 bg-gradient-to-br from-indigo-500 to-indigo-600 rounded-xl flex items-center justify-center shadow-indigo',
    iconClass: 'fas fa-chart-line',
    footerType: 'saving',
    savingText: t('dashboard.saved') + ' ' + formatSize(stats.value.savedSize)
  },
  {
    id: 'reuse-stats',
    title: t('dashboard.reuseStats'),
    value: stats.value.reuseRatio + '%',
    iconContainerClass: 'w-10 h-10 rounded-2xl bg-gradient-to-br from-pink-50 to-pink-100 flex items-center justify-center text-pink-500',
    iconWrapperClass: 'w-8 h-8 bg-gradient-to-br from-pink-500 to-pink-600 rounded-xl flex items-center justify-center shadow-pink',
    iconClass: 'fas fa-copy',
    footerType: 'reuse',
    reuseText: t('dashboard.reusedChunks') + ': ' + formatNumber(stats.value.reusedChunkCount) + ' ' + t('dashboard.pieces') 
  }
]);

// 获取统计数据
const fetchStats = async () => {
  try {
    const response = await getstats();
    const data = response.data || response;

    if (data.success !== false) {
      const accountStats = data.accountStats || {};
      const lastMonAccountStats = data.lastMonAccountStats || {};

      // 基本统计数据
      stats.value.bucketCount = accountStats.bucketCount || 0;
      stats.value.objectCount = accountStats.objectCount || 0;
      stats.value.blockCount = accountStats.blockCount || 0;
      stats.value.chunkCount = accountStats.chunkCountOfDedup || 0;
      stats.value.originalSize = accountStats.sizeOfObject || 0;
      stats.value.actualSize = accountStats.sizeOfBlock2 || 0;

      // 计算派生数据
      stats.value.savedSize = Math.max(0, stats.value.originalSize - stats.value.actualSize);
      stats.value.compressionRatio = stats.value.actualSize > 0 
        ? (stats.value.originalSize / stats.value.actualSize).toFixed(1) 
        : 0;

      stats.value.reusedChunkCount = Math.max(0, accountStats.chunkCount - (accountStats.chunkCountOfDedup || 0));
      stats.value.reusedSize = stats.value.savedSize;
      stats.value.reuseRatio = stats.value.chunkCount > 0 
        ? ((stats.value.reusedChunkCount / stats.value.chunkCount) * 100).toFixed(1) 
        : 0;

      // 计算增长率
      calculateGrowth(lastMonAccountStats);
    }
  } catch (error) {
    console.error('Failed to fetch stats:', error);
  }
};

// 计算增长率辅助函数
const calculateGrowth = (lastMonthStats) => {
  if (lastMonthStats.bucketCount) {
    stats.value.bucketGrowth = lastMonthStats.bucketCount > 0 
      ? Math.round(((stats.value.bucketCount - lastMonthStats.bucketCount) / lastMonthStats.bucketCount) * 100) 
      : 0;
  }

  if (lastMonthStats.objectCount) {
    stats.value.objectGrowth = lastMonthStats.objectCount > 0 
      ? Math.round(((stats.value.objectCount - lastMonthStats.objectCount) / lastMonthStats.objectCount) * 100) 
      : 0;
  }

  if (lastMonthStats.blockCount) {
    stats.value.blockGrowth = lastMonthStats.blockCount > 0 
      ? Math.round(((stats.value.blockCount - lastMonthStats.blockCount) / lastMonthStats.blockCount) * 100) 
      : 0;
  }

  if (lastMonthStats.chunkCount) {
    stats.value.chunkGrowth = lastMonthStats.chunkCount > 0 
      ? Math.round(((stats.value.chunkCount - lastMonthStats.chunkCount) / lastMonthStats.chunkCount) * 100) 
      : 0;
  }

  if (lastMonthStats.sizeOfObject) {
    stats.value.sizeGrowth = lastMonthStats.sizeOfObject > 0 
      ? Math.round(((stats.value.originalSize - lastMonthStats.sizeOfObject) / lastMonthStats.sizeOfObject) * 100) 
      : 0;
  }

  if (lastMonthStats.sizeOfChunk) {
    stats.value.actualSizeGrowth = lastMonthStats.sizeOfChunk > 0 
      ? Math.round(((stats.value.actualSize - lastMonthStats.sizeOfChunk) / lastMonthStats.sizeOfChunk) * 100) 
      : 0;
  }
};

// 组件挂载时获取数据
onMounted(() => {
  fetchStats();
});
</script>

<style scoped>
.dashboard-container {
  min-height: 100%;
  background: linear-gradient(135deg, #f8fafc 0%, #e2e8f0 100%);
  padding: 2rem;
}

/* 卡片样式 */
.stat-card {
  position: relative;
  overflow: hidden;
  transition: all 0.5s cubic-bezier(0.175, 0.885, 0.32, 1.275);
}

.stat-card:hover {
  transform: translateY(-5px);
}

/* 阴影定义 */
.shadow-card {
  box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.05), 0 2px 4px -1px rgba(0, 0, 0, 0.03);
}

.shadow-card-hover {
  box-shadow: 0 20px 25px -5px rgba(0, 0, 0, 0.08), 0 10px 10px -5px rgba(0, 0, 0, 0.02);
}

/* 图标阴影 */
.shadow-blue {
  box-shadow: 0 4px 6px -1px rgba(59, 130, 246, 0.3);
}

.shadow-purple {
  box-shadow: 0 4px 6px -1px rgba(139, 92, 246, 0.3);
}

.shadow-green {
  box-shadow: 0 4px 6px -1px rgba(34, 197, 94, 0.3);
}

.shadow-amber {
  box-shadow: 0 4px 6px -1px rgba(245, 158, 11, 0.3);
}

.shadow-red {
  box-shadow: 0 4px 6px -1px rgba(239, 68, 68, 0.3);
}

.shadow-cyan {
  box-shadow: 0 4px 6px -1px rgba(6, 182, 212, 0.3);
}

.shadow-indigo {
  box-shadow: 0 4px 6px -1px rgba(99, 102, 241, 0.3);
}

.shadow-pink {
  box-shadow: 0 4px 6px -1px rgba(236, 72, 153, 0.3);
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
}

/* 响应式设计 */
@media (max-width: 768px) {
  .dashboard-container {
    padding: 1rem;
  }

  .stat-card h3 {
    font-size: 1.8rem;
  }
}
</style>