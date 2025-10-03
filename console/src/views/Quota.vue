<template>
  <div class="quota-container">
    <!-- 头部标题和说明 -->
    <div class="storage-points-header mb-6 flex justify-between items-center px-4">
      <div>
        <h2 class="text-2xl font-bold text-gray-800">{{ t('quota.pageTitle') }}</h2>
        <p class="text-gray-500 mt-2">{{ t('quota.pageDescription') }}</p>
      </div>

      <!-- 按钮区域 -->
      <div class="flex gap-3">
        <button v-if="!isEditing" @click="startEditing(null)"
          class="px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white rounded-lg shadow transition-all duration-300 flex items-center gap-2">
          <i class="fas fa-plus"></i>
          <span>{{ t('quota.addQuota') }}</span>
        </button>
        <button v-else @click="cancelEditing"
          class="px-4 py-2 bg-amber-500 hover:bg-amber-600 text-white rounded-lg shadow transition-all duration-300 flex items-center gap-2">
          <i class="fas fa-times"></i>
          <span>{{ t('quota.cancelEditing') }}</span>
        </button>
      </div>
    </div>

    <!-- 配额列表卡片 -->
    <div class="config-card bg-white rounded-xl shadow-md p-6 mb-6">
      <!-- 无配额提示 -->
      <div v-if="quotas.length === 0" class="text-center py-10">
        <p class="text-gray-500 mb-4">{{ t('quota.noQuotas') }}</p>
        <button @click="startEditing(null)"
          class="px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white rounded-lg shadow transition-all duration-300 flex items-center gap-2 mx-auto">
          <i class="fas fa-plus"></i>
          <span>{{ t('quota.addQuota') }}</span>
        </button>
      </div>

      <!-- 配额列表 -->
      <div v-else class="overflow-x-auto">
        <table class="min-w-full divide-y divide-gray-200">
          <thead>
            <tr>
              <th class="px-6 py-3 text-left text-sm font-medium text-gray-500 uppercase tracking-wider">{{ t('quota.accountId') }}</th>
              <th class="px-6 py-3 text-left text-sm font-medium text-gray-500 uppercase tracking-wider">{{ t('quota.storageLimit') }}</th>
              <th class="px-6 py-3 text-left text-sm font-medium text-gray-500 uppercase tracking-wider">{{ t('quota.objectLimit') }}</th>
              <th class="px-6 py-3 text-left text-sm font-medium text-gray-500 uppercase tracking-wider">{{ t('quota.status') }}</th>
              <th class="px-6 py-3 text-left text-sm font-medium text-gray-500 uppercase tracking-wider">{{ t('quota.operation') }}</th>
            </tr>
          </thead>
          <tbody class="bg-white divide-y divide-gray-200">
            <tr v-for="quota in quotas" :key="quota.id">
              <td class="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">{{ quota.accountId }}</td>
              <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                {{ quota.storageLimit > 0 ? formatStorage(quota.storageLimit) : t('quota.unlimited') }}
              </td>
              <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                {{ quota.objectLimit > 0 ? quota.objectLimit.toLocaleString() : t('quota.unlimited') }}
              </td>
              <td class="px-6 py-4 whitespace-nowrap">
                <span class="px-2 py-1 inline-flex text-xs leading-5 font-semibold rounded-full"
                  :class="quota.isEnabled ? 'bg-green-100 text-green-800' : 'bg-red-100 text-red-800'">
                  {{ quota.isEnabled ? t('quota.enabled') : t('quota.disabled') }}
                </span>
              </td>
              <td class="px-6 py-4 whitespace-nowrap text-sm font-medium">
                <button @click="startEditing(quota)" class="text-blue-600 hover:text-blue-900 mr-3">
                  {{ t('quota.edit') }}
                </button>
                <button @click="handleDelete(quota)" class="text-red-600 hover:text-red-900">
                  {{ t('quota.delete') }}
                </button>
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>

    <!-- 配置表单卡片 - 仅在新增或编辑时显示 -->
    <div v-if="isEditing" class="config-card bg-white rounded-xl shadow-md p-6 mb-6">
      <!-- 表单内容 -->
      <form @submit.prevent="saveQuota" class="space-y-6">
        <div class="form-section">
          <h3 class="text-lg font-semibold text-gray-700 mb-4">{{ editingQuota.id ? t('quota.edit') : t('quota.addQuota') }}</h3>
          
          <div class="space-y-4">
            <div class="form-field">
              <label for="accountId" class="block text-sm font-medium text-gray-700 mb-1">{{ t('quota.accountId') }} *</label>
              <input
                id="accountId"
                v-model="editingQuota.accountId"
                type="text"
                :placeholder="t('quota.enterAccountId')"
                :disabled="!!editingQuota.id"
                class="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none transition-all duration-300"
              />
              <p class="text-xs text-gray-500 mt-1">{{ t('quota.accountIdDesc') }}</p>
            </div>

            <div class="form-field">
              <label class="block text-sm font-medium text-gray-700 mb-3">{{ t('quota.quotaConfig') }}</label>
              <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div>
                  <label for="storageLimit" class="block text-sm font-medium text-gray-700 mb-1">{{ t('quota.storageLimit') }}</label>
                  <input
                    id="storageLimit"
                    v-model.number="editingQuota.storageLimit"
                    type="number"
                    min="0"
                    placeholder="0"
                    class="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none transition-all duration-300"
                  />
                  <p class="text-xs text-gray-500 mt-1">{{ t('quota.zeroMeansUnlimited') }}</p>
                </div>
                <div>
                  <label for="objectLimit" class="block text-sm font-medium text-gray-700 mb-1">{{ t('quota.objectLimit') }}</label>
                  <input
                    id="objectLimit"
                    v-model.number="editingQuota.objectLimit"
                    type="number"
                    min="0"
                    placeholder="0"
                    class="w-full px-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none transition-all duration-300"
                  />
                  <p class="text-xs text-gray-500 mt-1">{{ t('quota.zeroMeansUnlimited') }}</p>
                </div>
              </div>
            </div>

            <div class="form-field">
              <label class="block text-sm font-medium text-gray-700 mb-1">{{ t('quota.statusConfig') }}</label>
              <div class="flex items-center mt-2">
                <label class="relative inline-flex items-center cursor-pointer">
                  <input
                    v-model="editingQuota.isEnabled"
                    type="checkbox"
                    class="sr-only peer"
                  />
                  <div class="w-11 h-6 bg-gray-200 peer-focus:outline-none peer-focus:ring-4 peer-focus:ring-blue-300 rounded-full peer peer-checked:after:translate-x-full peer-checked:after:border-white after:content-[''] after:absolute after:top-[2px] after:left-[2px] after:bg-white after:border-gray-300 after:border after:rounded-full after:h-5 after:w-5 after:transition-all peer-checked:bg-blue-600"></div>
                </label>
                <span class="ml-3 text-sm font-medium text-gray-700">{{ t('quota.enableQuota') }}</span>
              </div>
            </div>
          </div>
        </div>

        <!-- 操作按钮 -->
        <div class="form-actions flex justify-end gap-4 pt-4 border-t border-gray-100">
          <button type="button" @click="cancelEditing"
            class="px-4 py-2 border border-gray-300 text-gray-700 rounded-lg hover:bg-gray-50 transition-colors">
            {{ t('quota.cancel') }}
          </button>
          <button type="submit"
            class="px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white rounded-lg shadow-md hover:shadow-lg transition-all duration-300">
            {{ editingQuota.id ? t('quota.updateConfig') : t('quota.saveConfig') }}
          </button>
        </div>
      </form>
    </div>

    <!-- 操作结果提示 -->
    <div v-if="showToast"
      :class="['fixed top-4 right-4 px-4 py-3 rounded-lg shadow-lg transition-all duration-300 z-50', toastType === 'success' ? 'bg-green-500 text-white' : 'bg-red-500 text-white']">
      <div class="flex items-center gap-2">
        <i v-if="toastType === 'success'" class="fas fa-check-circle"></i>
        <i v-else class="fas fa-exclamation-circle"></i>
        <span>{{ toastMessage }}</span>
      </div>
    </div>

    <!-- 自定义确认对话框 -->
    <div v-if="showConfirmDialog" class="fixed inset-0 bg-[rgba(0,0,0,0.5)] flex items-center justify-center z-50 transition-opacity duration-300">
      <div class="bg-white rounded-xl shadow-xl w-full max-w-md mx-4 overflow-hidden transition-all duration-300 transform">
        <div class="p-5 border-b border-gray-100 flex items-center justify-between">
          <h3 class="text-lg font-bold text-gray-800">{{ confirmDialogTitle }}</h3>
          <button @click="closeConfirmDialog" class="text-gray-500 hover:text-gray-700 transition-colors" aria-label="关闭">
            <i class="fas fa-times"></i>
          </button>
        </div>
        <div class="p-5">
          <p class="text-gray-700">{{ confirmDialogMessage }}</p>
        </div>
        <div class="p-5 border-t border-gray-100 flex items-center justify-end gap-3">
          <button @click="closeConfirmDialog" class="px-4 py-2 border border-gray-300 rounded-lg text-gray-700 hover:bg-gray-50 transition-colors">
            {{ t('quota.cancel') }}
          </button>
          <button @click="confirmDialogAction" class="px-4 py-2 bg-red-600 text-white rounded-lg hover:bg-red-700 transition-colors">
            {{ t('quota.confirm') }}
          </button>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
export default {
  name: 'Quota',
  data() {
    return {
      isEditing: false,
      editingQuota: {
        id: null,
        accountId: '',
        storageLimit: 0,
        objectLimit: 0,
        isEnabled: true,
      },
      showToast: false,
      toastMessage: '',
      toastType: 'success',
      showConfirmDialog: false,
      confirmDialogTitle: '',
      confirmDialogMessage: '',
      confirmDialogAction: () => {},
      quotas: [],
    };
  },
  mounted() {
    this.fetchQuotas();
  },
  methods: {
    // ========== 工具函数 ==========
    /**
     * 保存到 localStorage
     */
    saveToStorage() {
      try {
        localStorage.setItem('quotas', JSON.stringify(this.quotas));
      } catch (err) {
        console.error('持久化失败:', err);
        this.showToastMessage('保存失败：本地存储已满或受限', 'error');
      }
    },

    /**
     * 从 localStorage 加载
     */
    fetchQuotas() {
      try {
        const saved = localStorage.getItem('quotas');
        if (saved) {
          this.quotas = JSON.parse(saved);
        } else {
          // 默认模拟数据
          this.quotas = [
            { id: '1', accountId: 'admin001', storageLimit: 0, objectLimit: 0, isEnabled: true },
            { id: '2', accountId: 'user001', storageLimit: 100, objectLimit: 100000, isEnabled: true },
            { id: '3', accountId: 'user002', storageLimit: 500, objectLimit: 500000, isEnabled: false },
          ];
          this.saveToStorage();
        }
      } catch (err) {
        console.warn('加载失败，使用默认数据:', err);
        this.quotas = [];
      }
    },

    /**
     * 添加配额
     */
    addQuota(quota) {
      const newQuota = {
        ...quota,
        id: Date.now().toString(), // 生成唯一ID
      };
      this.quotas.push(newQuota);
      this.saveToStorage();
    },

    /**
     * 更新配额
     */
    updateQuota(quota) {
      const index = this.quotas.findIndex(q => q.id === quota.id);
      if (index !== -1) {
        this.quotas[index] = { ...quota };
        this.saveToStorage();
      }
    },

    /**
     * 删除配额
     */
    deleteQuota(id) {
      const index = this.quotas.findIndex(q => q.id === id);
      if (index !== -1) {
        this.quotas.splice(index, 1);
        this.saveToStorage();
      }
    },
    
    startEditing(quota) {
      if (quota) {
        this.editingQuota = { ...quota };
      } else {
        this.editingQuota = {
          id: null,
          accountId: '',
          storageLimit: 0,
          objectLimit: 0,
          isEnabled: true,
        };
      }
      this.isEditing = true;
    },
    
    cancelEditing() {
      this.isEditing = false;
      this.editingQuota = {
        id: null,
        accountId: '',
        storageLimit: 0,
        objectLimit: 0,
        isEnabled: true,
      };
    },
    
    async saveQuota() {
      if (!this.editingQuota.accountId.trim()) {
        this.showToastMessage(this.t('quota.enterAccountId'), 'error');
        return;
      }
      
      try {
        const { accountId, storageLimit, objectLimit } = this.editingQuota;

        // 验证
        if (!accountId.trim()) throw new Error(this.t('quota.enterAccountId'));
        if (storageLimit < 0) throw new Error('存储空间限制不能为负数');
        if (objectLimit < 0) throw new Error('对象个数限制不能为负数');

        const trimmedId = accountId.trim();

        if (this.editingQuota.id) {
          // 更新
          const idx = this.quotas.findIndex(q => q.id === this.editingQuota.id);
          if (idx !== -1) {
            this.quotas[idx] = { ...this.editingQuota, accountId: trimmedId };
            this.saveToStorage();
            this.showToastMessage(this.t('quota.updateSuccess'), 'success');
          }
        } else {
          // 新增
          if (this.quotas.some(q => q.accountId === trimmedId)) {
            throw new Error('账号ID已存在，请使用其他ID');
          }
          this.addQuota({ ...this.editingQuota, accountId: trimmedId });
          this.showToastMessage(this.t('quota.addSuccess'), 'success');
        }
        this.isEditing = false;
      } catch (error) {
        this.showToastMessage(error.message || '操作失败', 'error');
      }
    },
    
    handleDelete(quota) {
      this.confirmDialogTitle = this.t('quota.confirmDeleteTitle');
      this.confirmDialogMessage = this.t('quota.confirmDeleteMessage');
      this.confirmDialogAction = async () => {
        try {
          this.deleteQuota(quota.id);
          this.showToastMessage(this.t('quota.deleteSuccess'), 'success');
        } catch (error) {
          this.showToastMessage(error.message || '删除失败', 'error');
        }
        this.closeConfirmDialog();
      };
      this.showConfirmDialog = true;
    },
    
    closeConfirmDialog() {
      this.showConfirmDialog = false;
      this.confirmDialogAction = () => {};
    },
    
    formatStorage(bytes) {
      if (bytes === 0) return '0 Bytes';
      
      const k = 1024;
      const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
      const i = Math.floor(Math.log(bytes) / Math.log(k));
      
      return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
    },
    
    showToastMessage(message, type = 'info') {
      // 创建临时toast元素
      const toast = document.createElement('div');
      toast.className = `toast ${type === 'success' ? 'bg-green-500' : 'bg-red-500'}`;
      toast.innerHTML = `
        <div class="flex items-center gap-2">
          <i class="fas ${type === 'success' ? 'fa-check-circle' : 'fa-exclamation-circle'}"></i>
          <span>${message}</span>
        </div>
      `;
      
      // 添加toast样式
      toast.style.position = 'fixed';
      toast.style.top = '1rem';
      toast.style.right = '1rem';
      toast.style.padding = '0.75rem 1rem';
      toast.style.borderRadius = '0.5rem';
      toast.style.color = 'white';
      toast.style.boxShadow = '0 10px 15px -3px rgba(0, 0, 0, 0.1)';
      toast.style.zIndex = '50';
      toast.style.display = 'flex';
      toast.style.alignItems = 'center';
      toast.style.gap = '0.5rem';
      toast.style.opacity = '0';
      toast.style.transform = 'translateX(20px)';
      toast.style.transition = 'opacity 0.3s, transform 0.3s';
      
      document.body.appendChild(toast);
      
      // 显示toast
      setTimeout(() => {
        toast.style.opacity = '1';
        toast.style.transform = 'translateX(0)';
      }, 10);
      
      // 3秒后隐藏toast
      setTimeout(() => {
        toast.style.opacity = '0';
        toast.style.transform = 'translateX(20px)';
        
        setTimeout(() => {
          if (document.body.contains(toast)) {
            document.body.removeChild(toast);
          }
        }, 300);
      }, 3000);
    },
    
    t(key) {
      return this.$t(key);
    },
  },
};
</script>

<style scoped>
.quota-container {
  max-width: 1200px;
  margin: 0 auto;
  padding: 20px;
}

/* 复用 EndPoint.vue 的样式类 */
.storage-points-header {
  margin-bottom: 24px;
}

.config-card {
  background-color: white;
  border-radius: 12px;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
  padding: 24px;
  margin-bottom: 24px;
}

.form-section {
  margin-bottom: 24px;
}

.form-field {
  margin-bottom: 16px;
}

.form-actions {
  display: flex;
  justify-content: flex-end;
  gap: 12px;
  padding-top: 16px;
  border-top: 1px solid #f0f0f0;
}

/* 响应式调整 */
@media (max-width: 768px) {
  .quota-container {
    padding: 16px;
  }
  
  .config-card {
    padding: 16px;
  }
  
  .grid-cols-1.md\:grid-cols-2 {
    grid-template-columns: 1fr;
  }
}
</style>