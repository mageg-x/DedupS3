<template>
  <div class="data-migration-container">
    <el-card class="mt-4">
      <template #header>
        <div class="card-header">
          <span>{{ t('advancedFeatures.migration') }}</span>
        </div>
      </template>
      <div class="card-content">
        <p>{{ t('advancedFeatures.migrationDescription') }}</p>
        <!-- 数据迁移功能实现将在这里 -->
      </div>
    </el-card>
  </div>
</template>

<script setup>
import { useI18n } from 'vue-i18n';

const { t } = useI18n();
</script>

<style scoped>
.data-migration-container {
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