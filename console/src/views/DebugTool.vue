<template>
  <div class="debug-tool-container">
    <el-card class="mt-4">
      <template #header>
        <div class="card-header">
          <span>{{ t('advancedFeatures.debugTool') }}</span>
        </div>
      </template>
      <div class="card-content">
        <p>{{ t('advancedFeatures.debugToolDescription') }}</p>
        <!-- 调试工具功能实现将在这里 -->
      </div>
    </el-card>
  </div>
</template>

<script setup>
import { useI18n } from 'vue-i18n';

const { t } = useI18n();
</script>

<style scoped>
.debug-tool-container {
  padding: 20px;
}

.card-header {
  font-weight: bold;
  font-size: 16px;
}

.card-content {
  padding: 20px 0;
}
</style>