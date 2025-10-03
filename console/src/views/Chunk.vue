<template>
  <div class="chunk-config-container">
    <!-- 头部标题和说明 -->
    <div class="chunk-config-header">
      <h2 class="text-2xl font-bold text-gray-800">{{ t('chunk.pageTitle') }}</h2>
      <p class="text-gray-500 mt-2">
        {{ t('chunk.configPlaceholder') }}
      </p>
    </div>

    <!-- 配置表单卡片 -->
    <div class="config-card bg-white rounded-xl shadow-md p-6">
      <!-- 表单内容 -->
      <form @submit.prevent="handleSave" class="space-y-8">
        <!-- 切片长度设置 -->
        <div class="form-section space-y-4">
          <h3 class="text-lg font-semibold text-gray-700 border-b border-gray-100 pb-2">
            {{ t('chunk.basicConfiguration') }}
          </h3>
          <div class="form-field space-y-2">
            <label for="chunkSize" class="block text-sm font-medium text-gray-700">
              {{ t('chunk.chunkLength') }}
            </label>
            <div class="flex items-center gap-3">
              <input id="chunkSize" v-model.number="chunkSize" type="number" min="1" max="1024"
                class="flex-1 max-w-48 px-4  py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none transition-all duration-300" />
              <select v-model="chunkSizeUnit"
                class="px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none transition-all duration-300 bg-white">
                <option value="MB">MB</option>
                <option value="KB">KB</option>
              </select>
            </div>
            <p class="text-xs text-gray-500">
              {{ t('chunk.recommendedValue') }}
            </p>
          </div>
        </div>

        <!-- 高级选项 -->
        <div class="form-section space-y-4">
          <h3 class="text-lg font-semibold text-gray-700 border-b border-gray-100 pb-2">
            {{ t('chunk.advancedSettings') }}
          </h3>
          <div class="grid grid-cols-1 md:grid-cols-3 gap-6">
            <!-- 固定长度选项 -->
            <div
              class="form-field space-y-2 p-4 bg-gray-50 rounded-lg hover:bg-gray-100 transition-colors duration-200">
              <label class="flex items-center justify-between text-sm font-medium text-gray-700">
                <span>{{ t('chunk.useFixedLengthChunking') }}</span>
                <el-switch v-model="useFixedLength" active-color="#409EFF" inactive-color="#DCDFE6" />
              </label>
              <p class="text-xs text-gray-500">
                {{ t('chunk.configPlaceholder') }}
              </p>
            </div>

            <!-- 加密选项 -->
            <div
              class="form-field space-y-2 p-4 bg-gray-50 rounded-lg hover:bg-gray-100 transition-colors duration-200">
              <label class="flex items-center justify-between text-sm font-medium text-gray-700">
                <span>{{ t('chunk.enableDataEncryption') }}</span>
                <el-switch v-model="enableEncryption" active-color="#409EFF" inactive-color="#DCDFE6" />
              </label>
              <p class="text-xs text-gray-500">
                {{ t('chunk.configPlaceholder') }}
              </p>
            </div>

            <!-- 压缩选项 -->
            <div
              class="form-field space-y-2 p-4 bg-gray-50 rounded-lg hover:bg-gray-100 transition-colors duration-200">
              <label class="flex items-center justify-between text-sm font-medium text-gray-700">
                <span>{{ t('chunk.enableDataCompression') }}</span>
                <el-switch v-model="enableCompression" active-color="#409EFF" inactive-color="#DCDFE6" />
              </label>
              <p class="text-xs text-gray-500">
                {{ t('chunk.configPlaceholder') }}
              </p>
            </div>
          </div>
        </div>

        <!-- 操作按钮 -->
        <div class="form-actions flex justify-end pt-4 border-t border-gray-100">
          <button type="submit"
            class="px-6 py-2 bg-blue-600 hover:bg-blue-700 text-white rounded-lg shadow hover:shadow-lg transition-all duration-300 flex items-center gap-2">
            <i class="fas fa-save"></i>
            <span>{{ t('chunk.saveConfiguration') }}</span>
          </button>
        </div>
      </form>
    </div>

    <!-- 操作结果提示 -->
    <div v-if="showToast" :class="[
      'fixed top-4 right-4 px-6 py-3 rounded-lg shadow-lg transition-all duration-300 z-50 flex items-center gap-2',
      toastType === 'success' ? 'bg-green-500 text-white' : 'bg-red-500 text-white']">
      <i v-if="toastType === 'success'" class="fas fa-check-circle"></i>
      <i v-else class="fas fa-exclamation-circle"></i>
      <span>{{ toastMessage }}</span>
    </div>
  </div>
</template>

<script setup>
import { ref } from 'vue';
import { useI18n } from 'vue-i18n';

// 获取翻译函数
const { t } = useI18n();

// 切片长度设置
const chunkSize = ref(32);
const chunkSizeUnit = ref('MB');

// 高级选项
const useFixedLength = ref(true);
const enableEncryption = ref(false);
const enableCompression = ref(true);

// 提示框状态
const showToast = ref(false);
const toastMessage = ref('');
const toastType = ref('success');

// 保存配置
const handleSave = () => {
  try {
    if (!chunkSize.value || chunkSize.value <= 0) {
      throw new Error('请输入有效的切片长度');
    }

    const config = {
      chunkSize: chunkSize.value,
      chunkSizeUnit: chunkSizeUnit.value,
      useFixedLength: useFixedLength.value,
      enableEncryption: enableEncryption.value,
      enableCompression: enableCompression.value,
      updatedAt: new Date().toISOString()
    };

    console.log('保存数据切片配置:', config);
    showToastMessage(t('chunk.saveSuccess'), 'success');
  } catch (error) {
    showToastMessage(error.message, 'error');
  }
};

// 显示提示消息
const showToastMessage = (message, type = 'success') => {
  toastMessage.value = message;
  toastType.value = type;
  showToast.value = true;

  setTimeout(() => {
    showToast.value = false;
  }, 3000);
};
</script>

<style scoped>
.chunk-config-container {
  background-color: #f5f7fa;
  min-height: calc(100vh - 120px);
  padding: 2rem;
}

.chunk-config-header {
  margin-bottom: 2rem;
}

.config-card {
  background: white;
  border-radius: 12px;
  box-shadow: 0 4px 20px rgba(0, 0, 0, 0.08);
  transition: all 0.3s ease;
}

.config-card:hover {
  box-shadow: 0 8px 30px rgba(0, 0, 0, 0.12);
}

/* 响应式调整 */
@media (max-width: 768px) {
  .chunk-config-container {
    padding: 1rem;
  }
}
</style>