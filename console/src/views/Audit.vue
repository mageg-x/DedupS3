<template>
  <div class="audits-container">
    <!-- 页面标题和操作按钮 -->
    <div class="page-header flex items-center justify-between mb-6">
      <h1 class="text-2xl font-bold text-gray-800">{{ t('audit.pageTitle') }}</h1>
      <div class="flex items-center gap-3">
        <div class="relative">
          <select v-model="auditTypeFilter"
            class="appearance-none text-sm bg-white border border-gray-300 rounded-lg pl-4 pr-10 py-2 focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-all">
            <option value="all">{{ t('auditTypes.all') }}</option>
            <!-- 对象操作 -->
            <option value="s3:GetObject">{{ t('auditTypes.s3:GetObject') }}</option>
            <option value="s3:PutObject">{{ t('auditTypes.s3:PutObject') }}</option>
            <option value="s3:DeleteObject">{{ t('auditTypes.s3:DeleteObject') }}</option>
            <option value="s3:GetObjectAcl">{{ t('auditTypes.s3:GetObjectAcl') }}</option>
            <option value="s3:PutObjectAcl">{{ t('auditTypes.s3:PutObjectAcl') }}</option>
            <option value="s3:GetObjectTagging">{{ t('auditTypes.s3:GetObjectTagging') }}</option>
            <option value="s3:PutObjectTagging">{{ t('auditTypes.s3:PutObjectTagging') }}</option>
            <option value="s3:GetObjectVersion">{{ t('auditTypes.s3:GetObjectVersion') }}</option>
            <option value="s3:DeleteObjectVersion">{{ t('auditTypes.s3:DeleteObjectVersion') }}</option>
            <option value="s3:GetObjectAttributes">{{ t('auditTypes.s3:GetObjectAttributes') }}</option>

            <!-- 存储桶操作 -->
            <option value="s3:CreateBucket">{{ t('auditTypes.s3:CreateBucket') }}</option>
            <option value="s3:DeleteBucket">{{ t('auditTypes.s3:DeleteBucket') }}</option>
            <option value="s3:ListBucket">{{ t('auditTypes.s3:ListBucket') }}</option>
            <option value="s3:ListBucketVersions">{{ t('auditTypes.s3:ListBucketVersions') }}</option>
            <option value="s3:GetBucketLocation">{{ t('auditTypes.s3:GetBucketLocation') }}</option>

            <!-- 访问控制操作 -->
            <option value="s3:GetBucketAcl">{{ t('auditTypes.s3:GetBucketAcl') }}</option>
            <option value="s3:PutBucketAcl">{{ t('auditTypes.s3:PutBucketAcl') }}</option>
            <option value="s3:GetBucketPolicy">{{ t('auditTypes.s3:GetBucketPolicy') }}</option>
            <option value="s3:PutBucketPolicy">{{ t('auditTypes.s3:PutBucketPolicy') }}</option>
            <option value="s3:DeleteBucketPolicy">{{ t('auditTypes.s3:DeleteBucketPolicy') }}</option>

            <!-- 高级配置操作 -->
            <option value="s3:PutBucketVersioning">{{ t('auditTypes.s3:PutBucketVersioning') }}</option>
            <option value="s3:GetBucketVersioning">{{ t('auditTypes.s3:GetBucketVersioning') }}</option>
            <option value="s3:PutBucketCORS">{{ t('auditTypes.s3:PutBucketCORS') }}</option>
            <option value="s3:GetBucketCORS">{{ t('auditTypes.s3:GetBucketCORS') }}</option>
            <option value="s3:PutBucketLogging">{{ t('auditTypes.s3:PutBucketLogging') }}</option>
            <option value="s3:GetBucketLogging">{{ t('auditTypes.s3:GetBucketLogging') }}</option>
            <option value="s3:PutBucketNotification">{{ t('auditTypes.s3:PutBucketNotification') }}</option>
            <option value="s3:GetBucketNotification">{{ t('auditTypes.s3:GetBucketNotification') }}</option>
            <option value="s3:PutBucketReplication">{{ t('auditTypes.s3:PutBucketReplication') }}</option>
            <option value="s3:GetBucketReplication">{{ t('auditTypes.s3:GetBucketReplication') }}</option>
            <option value="s3:PutBucketEncryption">{{ t('auditTypes.s3:PutBucketEncryption') }}</option>
            <option value="s3:GetBucketEncryption">{{ t('auditTypes.s3:GetBucketEncryption') }}</option>

            <!-- 生命周期配置操作 -->
            <option value="s3:PutBucketLifecycleConfiguration">{{ t('auditTypes.s3:PutBucketLifecycleConfiguration') }}</option>
            <option value="s3:GetBucketLifecycleConfiguration">{{ t('auditTypes.s3:GetBucketLifecycleConfiguration') }}</option>
            <option value="s3:DeleteBucketLifecycle">{{ t('auditTypes.s3:DeleteBucketLifecycle') }}</option>

            <!-- IAM操作 -->
            <option value="iam:CreateUser">{{ t('auditTypes.iam:CreateUser') }}</option>
            <option value="iam:DeleteUser">{{ t('auditTypes.iam:DeleteUser') }}</option>
            <option value="iam:UpdateUser">{{ t('auditTypes.iam:UpdateUser') }}</option>
            <option value="iam:CreateRole">{{ t('auditTypes.iam:CreateRole') }}</option>
            <option value="iam:DeleteRole">{{ t('auditTypes.iam:DeleteRole') }}</option>
            <option value="iam:UpdateRole">{{ t('auditTypes.iam:UpdateRole') }}</option>
            <option value="iam:AttachUserPolicy">{{ t('auditTypes.iam:AttachUserPolicy') }}</option>
            <option value="iam:DetachUserPolicy">{{ t('auditTypes.iam:DetachUserPolicy') }}</option>
            <option value="iam:AttachRolePolicy">{{ t('auditTypes.iam:AttachRolePolicy') }}</option>
            <option value="iam:DetachRolePolicy">{{ t('auditTypes.iam:DetachRolePolicy') }}</option>
            <option value="iam:CreateAccessKey">{{ t('auditTypes.iam:CreateAccessKey') }}</option>
            <option value="iam:DeleteAccessKey">{{ t('auditTypes.iam:DeleteAccessKey') }}</option>
          </select>
          <div class="pointer-events-none absolute inset-y-0 right-0 flex items-center px-2 text-gray-500">
            <i class="fas fa-chevron-down text-xs"></i>
          </div>
        </div>
        <button @click="exportAudits"
          class="flex items-center gap-1 px-3 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors">
          <i class="fas fa-download"></i>
          <span>{{ t('audit.export') }}</span>
        </button>
      </div>
    </div>

    <!-- 搜索框 -->
    <div class="search-container mb-6 text-sm">
      <div class="relative">
        <input type="text" v-model="searchKeyword" :placeholder="t('audit.searchPlaceholder')"
          class=" text-sm w-full px-4 py-2 pl-10 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-all">
        <i class="fas fa-search absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400"></i>
      </div>
    </div>

    <!-- 时间范围选择和风险级别筛选 -->
    <div class="filter-row  mb-6 flex flex-wrap gap-4 items-center">
      <div class="flex items-center gap-2">
        <label class="text-sm font-medium text-gray-700">{{ t('audit.startTime') }}</label>
        <el-date-picker
          v-model="startDate"
          type="datetime"
          format="YYYY-MM-DD HH:mm:ss"
          value-format="YYYY-MM-DDTHH:mm:ss"
          :placeholder="t('common.selectStartTime')"
          class="w-full text-[0.8rem]"
        />
      </div>
      <div class="flex items-center gap-2">
        <label class="text-sm font-medium text-gray-700">{{ t('audit.endTime') }}</label>
        <el-date-picker
          v-model="endDate"
          type="datetime"
          format="YYYY-MM-DD HH:mm:ss"
          value-format="YYYY-MM-DDTHH:mm:ss"
          :placeholder="t('common.selectEndTime')"
          class="w-full text-[0.8rem]"
        />
      </div>
      <div class="flex items-center gap-2">
        <label class="text-sm font-medium text-gray-700">{{ t('audit.riskLevel') }}</label>
        <div class="flex gap-2">
          <label class="inline-flex items-center">
            <input type="checkbox" v-model="riskLevelFilters" value="low"
              class="form-checkbox h-4 w-4 text-blue-600 rounded border-gray-300 focus:ring-blue-500">
            <span class="ml-2 text-sm text-gray-700">{{ t('audit.low') }}</span>
          </label>
          <label class="inline-flex items-center">
            <input type="checkbox" v-model="riskLevelFilters" value="medium"
              class="form-checkbox h-4 w-4 text-blue-600 rounded border-gray-300 focus:ring-blue-500">
            <span class="ml-2 text-sm text-gray-700">{{ t('audit.medium') }}</span>
          </label>
          <label class="inline-flex items-center">
            <input type="checkbox" v-model="riskLevelFilters" value="high"
              class="form-checkbox h-4 w-4 text-blue-600 rounded border-gray-300 focus:ring-blue-500">
            <span class="ml-2 text-sm text-gray-700">{{ t('audit.high') }}</span>
          </label>
        </div>
      </div>
    </div>

    <!-- 审计记录列表 -->
    <div class="card bg-white rounded-xl shadow-sm overflow-hidden">
      <div class="overflow-x-auto">
        <table class="w-full">
          <thead class="bg-gray-50 border-b border-gray-200">
            <tr>
              <th class="px-6 py-3 text-left text-sm font-medium text-gray-500 uppercase tracking-wider w-32">{{ t('audit.time') }}</th>
              <th class="px-6 py-3 text-left text-sm font-medium text-gray-500 uppercase tracking-wider w-36">{{ t('audit.auditType') }}</th>
              <th class="px-6 py-3 text-left text-sm font-medium text-gray-500 uppercase tracking-wider w-28">{{ t('audit.actor') }}</th>
              <th class="px-6 py-3 text-left text-sm font-medium text-gray-500 uppercase tracking-wider">{{ t('audit.target') }}</th>
              <th class="px-6 py-3 text-left text-sm font-medium text-gray-500 uppercase tracking-wider w-36">{{ t('audit.riskLevel') }}</th>
              <th class="px-6 py-3 text-left text-sm font-medium text-gray-500 uppercase tracking-wider w-20">{{ t('audit.status') }}</th>
              <th class="px-6 py-3 text-right text-sm font-medium text-gray-500 uppercase tracking-wider w-32">{{ t('audit.operation') }}</th>
            </tr>
          </thead>
          <tbody class=" text-sm bg-white divide-y divide-gray-200">
            <tr v-for="audit in paginatedAudits" :key="audit.id" class="hover:bg-gray-50 transition-colors">
              <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                {{ formatDate(audit.timestamp) }}
              </td>
              <td class="px-6 py-4 whitespace-nowrap">
                <span :class="['px-2 py-1 text-xs rounded-full', getAuditTypeClass(audit.type)]">
                  {{ getAuditTypeName(audit.type) }}
                </span>
              </td>
              <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-900 min-w-30">
                {{ audit.actor || t('common.system') }}
              </td>
              <td class="px-6 py-4 text-sm text-gray-700 ">
                {{ audit.target || 'N/A' }}
              </td>
              <td class="px-6 py-4 whitespace-nowrap min-w-40">
                <span :class="['px-2 py-1 text-xs rounded-full ', getRiskLevelClass(audit.riskLevel)]">
                  {{ getRiskLevelName(audit.riskLevel) }}
                </span>
              </td>
              <td class="px-6 py-4 whitespace-nowrap">
                <span
                  :class="['px-2 py-1 text-xs rounded-full', audit.status === 'success' ? 'bg-green-100 text-green-800' : 'bg-red-100 text-red-800']">
                  {{ audit.status === 'success' ? t('common.success') : t('common.failed') }}
                </span>
              </td>
              <td class="px-6 py-4 whitespace-nowrap text-right text-sm font-medium">
                <button @click="showAuditDetails(audit)"
                  class="text-blue-600 hover:text-blue-900 transition-colors mr-3">
                  <i class="fas fa-eye mr-1"></i>{{ t('common.details') }}
                </button>
                <button @click="generateReport(audit)" class="text-green-600 hover:text-green-900 transition-colors">
                  <i class="fas fa-file-alt mr-1"></i>{{ t('common.report') }}
                </button>
              </td>
            </tr>
          </tbody>
        </table>
      </div>
      <!-- 空状态 -->
      <div v-if="filteredAudits.length === 0" class="py-12 text-center">
        <div class="text-gray-400 mb-4">
          <i class="fas fa-file-audio text-4xl"></i>
        </div>
        <h3 class="text-lg font-medium text-gray-900 mb-2">{{ t('audit.noAuditRecords') }}</h3>
        <p class="text-gray-500 mb-6">{{ t('audit.systemAutomaticallyRecords') }}</p>
      </div>
    </div>

    <!-- 分页控件 -->
    <div v-if="totalPages > 0" class="flex items-center justify-between mt-4">
      <div class="flex items-center space-x-2">
        <span class="text-sm text-gray-600">{{ t('common.totalRecords', {total: filteredAudits.length, size: pageSize}) }}</span>
        <select v-model="pageSize" @change="changePageSize" class="border border-gray-300 rounded-md text-sm">
          <option value="10">{{ t('common.totalRecords', {total: 10, size: 10}) }}</option>
          <option value="20">{{ t('common.totalRecords', {total: 20, size: 20}) }}</option>
          <option value="50">{{ t('common.totalRecords', {total: 50, size: 50}) }}</option>
        </select>
      </div>
      <div class="flex items-center space-x-1">
        <button @click="changePage(currentPage - 1)" :disabled="currentPage === 1"
          class="px-3 py-1 text-sm border rounded-md hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed">
          <i class="fas fa-chevron-left"></i>
        </button>
        <span v-for="page in displayPages" :key="page" class="px-3 py-1 text-sm">
          <button v-if="page === '...'" class="px-2 py-1 text-sm text-gray-500" disabled>
            ...
          </button>
          <button v-else @click="changePage(page)" class="px-2 py-1 text-sm rounded-md transition-colors"
            :class="currentPage === page ? 'bg-blue-100 text-blue-800' : 'hover:bg-gray-100'">
            {{ page }}
          </button>
        </span>
        <button @click="changePage(currentPage + 1)" :disabled="currentPage === totalPages"
          class="px-3 py-1 text-sm border rounded-md hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed">
          <i class="fas fa-chevron-right"></i>
        </button>
      </div>
    </div>

    <!-- 审计详情对话框 -->
    <div v-if="detailsVisible" class="fixed inset-0 bg-[rgba(0,0,0,0.5)] flex items-center justify-center z-50">
      <div
        class="bg-white rounded-xl shadow-xl w-full max-w-3xl mx-4 overflow-hidden animate-fadeIn max-h-[90vh] flex flex-col">
        <div class="p-5 border-b border-gray-100 flex items-center justify-between">
          <h3 class="text-lg font-bold text-gray-800">{{ t('audit.detailsTitle') }}</h3>
          <button @click="closeDetails" class="text-gray-500 hover:text-gray-700 transition-colors" aria-label="关闭">
            <i class="fas fa-times"></i>
          </button>
        </div>
        <div class="p-5 flex-grow overflow-y-auto">
          <div class="space-y-4">
            <div class="flex justify-between">
              <span class="text-gray-500">{{ t('audit.time') }}:</span>
              <span class="font-medium">{{ formatDate(currentAudit.timestamp) }}</span>
            </div>
            <div class="flex justify-between">
              <span class="text-gray-500">{{ t('audit.apiOperation') }}:</span>
              <span :class="['px-2 py-1 text-xs rounded-full', getAuditTypeClass(currentAudit.type)]">
                {{ currentAudit.type }}
              </span>
            </div>
            <div class="flex justify-between">
              <span class="text-gray-500">{{ t('audit.operationDescription') }}:</span>
              <span class="font-medium">{{ getAuditTypeName(currentAudit.type) }}</span>
            </div>
            <div class="flex justify-between">
              <span class="text-gray-500">{{ t('audit.operator') }}:</span>
              <span class="font-medium">{{ currentAudit.actor || t('common.system') }}</span>
            </div>
            <div class="flex justify-between">
              <span class="text-gray-500">{{ t('audit.sourceIp') }}:</span>
              <span class="font-medium">{{ currentAudit.ipAddress || 'N/A' }}</span>
            </div>
            <div class="flex justify-between">
              <span class="text-gray-500">{{ t('audit.impactedResource') }}:</span>
              <span class="font-medium text-blue-600 hover:text-blue-800 cursor-pointer truncate max-w-[50%]">{{
                currentAudit.target || 'N/A' }}</span>
            </div>
            <div class="flex justify-between">
              <span class="text-gray-500">{{ t('audit.riskLevel') }}:</span>
              <span :class="['px-2 py-1 text-xs rounded-full', getRiskLevelClass(currentAudit.riskLevel)]">
                {{ getRiskLevelName(currentAudit.riskLevel) }}
              </span>
            </div>
            <div class="flex justify-between">
              <span class="text-gray-500">{{ t('audit.requestStatus') }}:</span>
              <span
                :class="['px-2 py-1 text-xs rounded-full', currentAudit.status === 'success' ? 'bg-green-100 text-green-800' : 'bg-red-100 text-red-800']">
                {{ currentAudit.status === 'success' ? t('common.success') : t('common.failure') }}
              </span>
            </div>
            <div v-if="currentAudit.status === 'failure'" class="">
              <div class="text-gray-500 mb-1">{{ t('audit.errorMessage') }}:</div>
              <div class="p-3 bg-red-50 rounded-lg text-sm text-red-700">
                <div><strong>{{ t('audit.errorCode') }}:</strong> {{ currentAudit.errorCode }}</div>
                <div><strong>{{ t('audit.errorDescription') }}:</strong> {{ currentAudit.errorMessage }}</div>
              </div>
            </div>
            <div class="">
              <div class="text-gray-500 mb-1">{{ t('audit.detailedDescription') }}:</div>
              <div class="p-3 bg-gray-50 rounded-lg text-sm">{{ currentAudit.description }}</div>
            </div>
            <div v-if="currentAudit.context" class="">
              <div class="text-gray-500 mb-1">{{ t('audit.awsContextInfo') }}:</div>
              <div class="p-3 bg-gray-50 rounded-lg text-sm font-mono whitespace-pre-wrap max-h-60 overflow-y-auto">
                {{ JSON.stringify(currentAudit.context, null, 2) }}
              </div>
            </div>
          </div>
        </div>
        <div class="p-5 border-t border-gray-100 flex items-center justify-end gap-3">
          <button @click="generateReport(currentAudit)"
            class="px-6 py-2 border border-gray-300 text-gray-700 rounded-lg hover:bg-gray-50 transition-colors">
            {{ t('audit.generateReport') }}
          </button>
          <button @click="closeDetails"
            class="px-6 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors">
            {{ t('common.close') }}
          </button>
        </div>
      </div>
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
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue';
import { useI18n } from 'vue-i18n';
import { ElDatePicker } from 'element-plus';

const { t } = useI18n();

// 模拟审计数据
const auditsList = ref([]);

// 搜索和筛选
const searchKeyword = ref('');
const auditTypeFilter = ref('all');
const startDate = ref('');
const endDate = ref('');
const riskLevelFilters = ref(['low', 'medium', 'high']);

// 分页相关
const currentPage = ref(1);
const pageSize = ref(10);

// 详情对话框
const detailsVisible = ref(false);
const currentAudit = ref({});

// 提示框状态
const showToast = ref(false);
const toastMessage = ref('');
const toastType = ref('success');

// 生成模拟审计数据（AWS S3风格）
const generateMockAudits = () => {
  const now = new Date();
  const auditTypes = [
    // 对象操作
    's3:GetObject', 's3:PutObject', 's3:DeleteObject',
    's3:GetObjectAcl', 's3:PutObjectAcl',
    's3:GetObjectTagging', 's3:PutObjectTagging',
    's3:GetObjectVersion', 's3:DeleteObjectVersion',
    's3:GetObjectAttributes',

    // 存储桶操作
    's3:CreateBucket', 's3:DeleteBucket',
    's3:ListBucket', 's3:ListBucketVersions',
    's3:GetBucketLocation',

    // 访问控制操作
    's3:GetBucketAcl', 's3:PutBucketAcl',
    's3:GetBucketPolicy', 's3:PutBucketPolicy', 's3:DeleteBucketPolicy',

    // 高级配置操作
    's3:PutBucketVersioning', 's3:GetBucketVersioning',
    's3:PutBucketCORS', 's3:GetBucketCORS',
    's3:PutBucketLogging', 's3:GetBucketLogging',
    's3:PutBucketNotification', 's3:GetBucketNotification',
    's3:PutBucketReplication', 's3:GetBucketReplication',
    's3:PutBucketEncryption', 's3:GetBucketEncryption',

    // 生命周期配置操作
    's3:PutBucketLifecycleConfiguration', 's3:GetBucketLifecycleConfiguration', 's3:DeleteBucketLifecycle',

    // IAM操作
    'iam:CreateUser', 'iam:DeleteUser', 'iam:UpdateUser',
    'iam:CreateRole', 'iam:DeleteRole', 'iam:UpdateRole',
    'iam:AttachUserPolicy', 'iam:DetachUserPolicy',
    'iam:AttachRolePolicy', 'iam:DetachRolePolicy',
    'iam:CreateAccessKey', 'iam:DeleteAccessKey'
  ];
  const actors = ['arn:aws:iam::123456789012:user/admin', 'arn:aws:iam::123456789012:user/bob', 'arn:aws:iam::123456789012:user/alice', 'arn:aws:iam::123456789012:role/s3-admin', 'arn:aws:iam::123456789012:assumed-role/app-role/1234567890'];
  const ipAddresses = ['192.168.1.100', '10.0.0.5', '172.16.0.23', '127.0.0.1', '54.240.143.128', '54.239.28.85'];
  const statuses = ['success', 'failure'];
  const riskLevels = ['low', 'medium', 'high'];

  const auditTemplates = {
    // 对象操作
    's3:GetObject': '{actor} 从存储桶 {bucket} 下载了对象 {object}',
    's3:PutObject': '{actor} 向存储桶 {bucket} 上传了对象 {object}',
    's3:DeleteObject': '{actor} 从存储桶 {bucket} 删除了对象 {object}',
    's3:GetObjectAcl': '{actor} 获取了存储桶 {bucket} 中对象 {object} 的访问控制列表',
    's3:PutObjectAcl': '{actor} 修改了存储桶 {bucket} 中对象 {object} 的访问控制列表',
    's3:GetObjectTagging': '{actor} 获取了存储桶 {bucket} 中对象 {object} 的标签',
    's3:PutObjectTagging': '{actor} 设置了存储桶 {bucket} 中对象 {object} 的标签',
    's3:GetObjectVersion': '{actor} 获取了存储桶 {bucket} 中对象 {object} 的特定版本',
    's3:DeleteObjectVersion': '{actor} 删除了存储桶 {bucket} 中对象 {object} 的特定版本',
    's3:GetObjectAttributes': '{actor} 获取了存储桶 {bucket} 中对象 {object} 的属性',

    // 存储桶操作
    's3:CreateBucket': '{actor} 创建了存储桶 {bucket}',
    's3:DeleteBucket': '{actor} 删除了存储桶 {bucket}',
    's3:ListBucket': '{actor} 列出了存储桶 {bucket} 中的对象',
    's3:ListBucketVersions': '{actor} 列出了存储桶 {bucket} 中的对象版本',
    's3:GetBucketLocation': '{actor} 获取了存储桶 {bucket} 的区域信息',

    // 访问控制操作
    's3:GetBucketAcl': '{actor} 获取了存储桶 {bucket} 的访问控制列表',
    's3:PutBucketAcl': '{actor} 修改了存储桶 {bucket} 的访问控制列表',
    's3:GetBucketPolicy': '{actor} 获取了存储桶 {bucket} 的策略',
    's3:PutBucketPolicy': '{actor} 设置了存储桶 {bucket} 的策略',
    's3:DeleteBucketPolicy': '{actor} 删除了存储桶 {bucket} 的策略',

    // 高级配置操作
    's3:PutBucketVersioning': '{actor} 设置了存储桶 {bucket} 的版本控制配置',
    's3:GetBucketVersioning': '{actor} 获取了存储桶 {bucket} 的版本控制状态',
    's3:PutBucketCORS': '{actor} 设置了存储桶 {bucket} 的跨域资源共享(CORS)规则',
    's3:GetBucketCORS': '{actor} 获取了存储桶 {bucket} 的跨域资源共享(CORS)规则',
    's3:PutBucketLogging': '{actor} 设置了存储桶 {bucket} 的访问日志记录配置',
    's3:GetBucketLogging': '{actor} 获取了存储桶 {bucket} 的访问日志记录配置',
    's3:PutBucketNotification': '{actor} 设置了存储桶 {bucket} 的事件通知配置',
    's3:GetBucketNotification': '{actor} 获取了存储桶 {bucket} 的事件通知配置',
    's3:PutBucketReplication': '{actor} 设置了存储桶 {bucket} 的跨区域复制规则',
    's3:GetBucketReplication': '{actor} 获取了存储桶 {bucket} 的跨区域复制规则配置',
    's3:PutBucketEncryption': '{actor} 设置了存储桶 {bucket} 的服务器端加密配置',
    's3:GetBucketEncryption': '{actor} 获取了存储桶 {bucket} 的服务器端加密配置',

    // 生命周期配置操作
    's3:PutBucketLifecycleConfiguration': '{actor} 设置了存储桶 {bucket} 的生命周期配置规则',
    's3:GetBucketLifecycleConfiguration': '{actor} 获取了存储桶 {bucket} 的生命周期配置规则',
    's3:DeleteBucketLifecycle': '{actor} 删除了存储桶 {bucket} 的生命周期配置规则',

    // IAM操作
    'iam:CreateUser': '{actor} 创建了IAM用户 {target}',
    'iam:DeleteUser': '{actor} 删除了IAM用户 {target}',
    'iam:UpdateUser': '{actor} 更新了IAM用户 {target} 的信息',
    'iam:CreateRole': '{actor} 创建了IAM角色 {target}',
    'iam:DeleteRole': '{actor} 删除了IAM角色 {target}',
    'iam:UpdateRole': '{actor} 更新了IAM角色 {target} 的信息',
    'iam:AttachUserPolicy': '{actor} 为IAM用户 {target} 附加了权限策略',
    'iam:DetachUserPolicy': '{actor} 为IAM用户 {target} 分离了权限策略',
    'iam:AttachRolePolicy': '{actor} 为IAM角色 {target} 附加了权限策略',
    'iam:DetachRolePolicy': '{actor} 为IAM角色 {target} 分离了权限策略',
    'iam:CreateAccessKey': '{actor} 为IAM用户 {target} 创建了访问密钥',
    'iam:DeleteAccessKey': '{actor} 为IAM用户 {target} 删除了访问密钥'
  };

  const buckets = ['mycompany-data', 'mycompany-logs', 'mycompany-backup', 'mycompany-archive', 'mycompany-public', 'project-x-dev', 'project-x-prod'];
  const objectTypes = ['documents/', 'images/', 'videos/', 'logs/', 'config/'];
  const fileExtensions = ['.txt', '.pdf', '.jpg', '.png', '.json', '.log', '.csv', '.xml'];

  const audits = [];

  // 生成过去30天的审计记录
  for (let i = 0; i < 80; i++) {
    const daysAgo = Math.floor(Math.random() * 30);
    const hoursAgo = Math.floor(Math.random() * 24);
    const minutesAgo = Math.floor(Math.random() * 60);
    const timestamp = new Date(now);
    timestamp.setDate(timestamp.getDate() - daysAgo);
    timestamp.setHours(timestamp.getHours() - hoursAgo);
    timestamp.setMinutes(timestamp.getMinutes() - minutesAgo);

    const type = auditTypes[Math.floor(Math.random() * auditTypes.length)];
    const actor = actors[Math.floor(Math.random() * actors.length)];
    const ipAddress = ipAddresses[Math.floor(Math.random() * ipAddresses.length)];
    const status = statuses[Math.floor(Math.random() * statuses.length)];

    // 根据操作类型设置风险级别
    let riskLevel = 'low';
    // 高风险操作 - 直接影响安全性或数据完整性的操作
    if (['s3:DeleteBucket', 's3:PutBucketAcl', 's3:PutBucketPolicy', 's3:DeleteBucketPolicy',
      's3:PutBucketVersioning', 'iam:DeleteUser', 'iam:DeleteRole', 'iam:AttachUserPolicy',
      'iam:AttachRolePolicy', 'iam:CreateAccessKey'].includes(type)) {
      riskLevel = Math.random() > 0.3 ? 'medium' : 'high';
    }
    // 中等风险操作 - 可能影响数据或权限的操作
    else if (['s3:DeleteObject', 's3:DeleteObjectVersion', 's3:PutBucketEncryption',
      's3:PutBucketReplication', 's3:DeleteBucketLifecycle', 'iam:DeleteAccessKey',
      's3:PutBucketLifecycleConfiguration', 's3:PutBucketLogging', 's3:PutBucketNotification'].includes(type)) {
      riskLevel = Math.random() > 0.5 ? 'medium' : 'low';
    }
    // 特殊处理某些操作的风险级别
    if (type === 's3:PutObjectAcl' && Math.random() > 0.4) {
      riskLevel = 'medium';
    }

    // 生成存储桶和对象名称
    const bucket = buckets[Math.floor(Math.random() * buckets.length)];
    const objectType = objectTypes[Math.floor(Math.random() * objectTypes.length)];
    const objectName = objectType + 'file-' + Math.random().toString(36).substr(2, 9) + fileExtensions[Math.floor(Math.random() * fileExtensions.length)];

    // 生成IAM相关资源名称
    const iamUsers = ['admin-user', 'dev-user', 'read-only-user', 'backup-user', 'analytics-user', 'app-user'];
    const iamRoles = ['s3-full-access', 'ec2-admin', 'data-analyst', 'backup-role', 'app-role'];
    const iamPolicies = ['arn:aws:iam::123456789012:policy/FullAccessPolicy', 'arn:aws:iam::123456789012:policy/ReadOnlyPolicy', 'arn:aws:iam::123456789012:policy/S3AccessPolicy'];

    // 根据审计类型生成描述和影响对象
    const template = auditTemplates[type];
    let description = template.replace('{actor}', actor.split('/').pop());
    let target = bucket; // 默认值

    if (type.startsWith('s3:')) {
      description = description.replace('{bucket}', bucket);
      if (template.includes('{object}')) {
        description = description.replace('{object}', objectName);
        target = bucket + '/' + objectName;
      }
    } else if (type.startsWith('iam:')) {
      // IAM操作特殊处理
      let iamResource = '';
      if (type.includes('User')) {
        iamResource = iamUsers[Math.floor(Math.random() * iamUsers.length)];
      } else if (type.includes('Role')) {
        iamResource = iamRoles[Math.floor(Math.random() * iamRoles.length)];
      }
      description = description.replace('{target}', iamResource);
      target = iamResource;
    }

    // 添加上下文信息（AWS风格）
    let context = {};

    if (type.startsWith('s3:')) {
      // S3操作上下文
      context = {
        requestId: '79104EXAMPLEB723',
        requestParameters: {
          bucketName: bucket
        },
        responseElements: {
          'x-amz-request-id': '79104EXAMPLEB723',
          'x-amz-id-2': 'eftixk72aD6Ap51TnqcoF8eFidJG9Z/2md4yiJ5v8/t6ftpxovnQSMrpnHrHBhD4'
        },
        userAgent: ['Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36',
          'aws-cli/2.4.10 Python/3.8.8 Linux/5.4.0-88-generic botocore/2.4.10',
          'S3Console/0.4, aws-internal/3 aws-sdk-java/1.12.196 Linux/5.4.174-97.354.amzn2int.x86_64 OpenJDK_64-Bit_Server_VM/25.312-b07 java/1.8.0_312 vendor/Oracle_Corporation'].sort(() => 0.5 - Math.random())[0],
        eventSource: 's3.amazonaws.com',
        eventTime: timestamp.toISOString(),
        eventName: type,
        awsRegion: ['us-east-1', 'us-west-1', 'us-west-2', 'eu-west-1', 'ap-northeast-1', 'ap-southeast-1'].sort(() => 0.5 - Math.random())[0],
        sourceIPAddress: ipAddress,
        recipientAccountId: '123456789012',
        requestPrincipalId: 'AIDAJQABLZS4A3QDU576Q',
        userIdentity: {
          type: actor.includes('role') ? 'AssumedRole' : 'IAMUser',
          principalId: 'AIDAJQABLZS4A3QDU576Q',
          arn: actor,
          accountId: '123456789012',
          userName: actor.split('/').pop()
        },
        s3: {
          s3SchemaVersion: '1.0',
          configurationId: 'tf-s3-logs-20230101',
          bucket: {
            name: bucket,
            ownerIdentity: {
              principalId: 'A3NL1KOZZKExample'
            },
            arn: `arn:aws:s3:::${bucket}`
          }
        }
      };
    } else if (type.startsWith('iam:')) {
      // IAM操作上下文
      context = {
        requestId: '79104EXAMPLEB723',
        requestParameters: {
          userName: target,
          roleName: target.includes('role') ? target : undefined
        },
        responseElements: {
          requestId: '79104EXAMPLEB723'
        },
        userAgent: ['Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36',
          'aws-cli/2.4.10 Python/3.8.8 Linux/5.4.0-88-generic botocore/2.4.10',
          'signin.amazonaws.com'].sort(() => 0.5 - Math.random())[0],
        eventSource: 'iam.amazonaws.com',
        eventTime: timestamp.toISOString(),
        eventName: type,
        awsRegion: 'us-east-1', // IAM是全局服务
        sourceIPAddress: ipAddress,
        recipientAccountId: '123456789012',
        requestPrincipalId: 'AIDAJQABLZS4A3QDU576Q',
        userIdentity: {
          type: actor.includes('role') ? 'AssumedRole' : 'IAMUser',
          principalId: 'AIDAJQABLZS4A3QDU576Q',
          arn: actor,
          accountId: '123456789012',
          userName: actor.split('/').pop()
        }
      };
    }

    // 如果是对象操作，添加对象信息
    if (template.includes('{object}')) {
      context.requestParameters.key = objectName;
      context.s3.object = {
        key: objectName,
        size: Math.floor(Math.random() * 1024 * 1024 * 10), // 0-10MB
        eTag: 'd41d8cd98f00b204e9800998ecf8427e',
        versionId: 'null',
        sequencer: '0A1B2C3D4E5F678901'
      };
    }

    // 如果是失败状态，添加错误信息
    let errorCode = null;
    let errorMessage = null;
    if (status === 'failure') {
      const errorTypes = [
        { code: 'AccessDenied', message: 'Access Denied' },
        { code: 'NoSuchBucket', message: 'The specified bucket does not exist' },
        { code: 'NoSuchKey', message: 'The specified key does not exist' },
        { code: 'InvalidArgument', message: 'Invalid Argument' },
        { code: 'SignatureDoesNotMatch', message: 'The request signature we calculated does not match the signature you provided' }
      ];
      const error = errorTypes[Math.floor(Math.random() * errorTypes.length)];
      errorCode = error.code;
      errorMessage = error.message;
      context.errorCode = error.code;
      context.errorMessage = error.message;
    }

    audits.push({
      id: i + 1,
      timestamp: timestamp,
      type: type,
      actor: actor.split('/').pop(),
      ipAddress: ipAddress,
      target: target,
      description: description,
      status: status,
      riskLevel: riskLevel,
      errorCode: errorCode,
      errorMessage: errorMessage,
      context: context,
      changes: type.includes('Put') || type.includes('Delete') ? {
        operation: type,
        timestamp: timestamp.toISOString(),
        resource: target
      } : null
    });
  }

  // 按时间倒序排列
  audits.sort((a, b) => b.timestamp - a.timestamp);
  return audits;
};

// 加载模拟数据
const loadMockData = () => {
  auditsList.value = generateMockAudits();
};

// 格式化日期
const formatDate = (date) => {
  if (!(date instanceof Date)) {
    date = new Date(date);
  }
  return date.toLocaleString('zh-CN', {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit'
  });
};

// 格式化变更值
const formatChangeValue = (value) => {
  if (value === null || value === undefined) {
    return 'null';
  }
  if (typeof value === 'object') {
    return JSON.stringify(value, null, 2);
  }
  return String(value);
};

// 获取审计类型名称
const getAuditTypeName = (type) => {
  // 使用i18n获取翻译后的审计类型名称
  return t(`auditTypes.${type}`) || type;
};

// 获取审计类型样式（S3操作类型）
const getAuditTypeClass = (type) => {
  const typeClasses = {
    // 对象操作 - 使用绿色系
    's3:GetObject': 'bg-green-100 text-green-800',
    's3:GetObjectAcl': 'bg-green-50 text-green-700',
    's3:GetObjectTagging': 'bg-green-50 text-green-700',
    's3:GetObjectVersion': 'bg-green-50 text-green-700',
    's3:GetObjectAttributes': 'bg-green-50 text-green-700',

    // 写操作 - 使用蓝色系
    's3:PutObject': 'bg-blue-100 text-blue-800',
    's3:PutObjectAcl': 'bg-blue-50 text-blue-700',
    's3:PutObjectTagging': 'bg-blue-50 text-blue-700',

    // 删除操作 - 使用红色系
    's3:DeleteObject': 'bg-red-100 text-red-800',
    's3:DeleteObjectVersion': 'bg-red-50 text-red-700',
    's3:DeleteBucket': 'bg-red-100 text-red-800',
    's3:DeleteBucketPolicy': 'bg-red-50 text-red-700',

    // 存储桶操作 - 使用紫色系
    's3:CreateBucket': 'bg-purple-100 text-purple-800',
    's3:ListBucket': 'bg-purple-50 text-purple-700',
    's3:ListBucketVersions': 'bg-purple-50 text-purple-700',
    's3:GetBucketLocation': 'bg-purple-50 text-purple-700',

    // 访问控制操作 - 使用橙色系
    's3:GetBucketAcl': 'bg-orange-50 text-orange-700',
    's3:PutBucketAcl': 'bg-orange-100 text-orange-800',
    's3:GetBucketPolicy': 'bg-orange-50 text-orange-700',
    's3:PutBucketPolicy': 'bg-orange-100 text-orange-800',

    // 高级配置操作 - 使用青色系
    's3:PutBucketVersioning': 'bg-teal-100 text-teal-800',
    's3:GetBucketVersioning': 'bg-teal-50 text-teal-700',
    's3:PutBucketCORS': 'bg-teal-100 text-teal-800',
    's3:GetBucketCORS': 'bg-teal-50 text-teal-700',
    's3:PutBucketLogging': 'bg-teal-100 text-teal-800',
    's3:GetBucketLogging': 'bg-teal-50 text-teal-700',
    's3:PutBucketNotification': 'bg-teal-100 text-teal-800',
    's3:GetBucketNotification': 'bg-teal-50 text-teal-700',
    's3:PutBucketReplication': 'bg-teal-100 text-teal-800',
    's3:GetBucketReplication': 'bg-teal-50 text-teal-700',
    's3:PutBucketEncryption': 'bg-teal-100 text-teal-800',
    's3:GetBucketEncryption': 'bg-teal-50 text-teal-700',

    // 生命周期配置操作 - 使用靛蓝色系
    's3:PutBucketLifecycleConfiguration': 'bg-indigo-100 text-indigo-800',
    's3:GetBucketLifecycleConfiguration': 'bg-indigo-50 text-indigo-700',
    's3:DeleteBucketLifecycle': 'bg-indigo-100 text-indigo-800',

    // IAM操作 - 使用紫色系
    'iam:CreateUser': 'bg-purple-100 text-purple-800',
    'iam:DeleteUser': 'bg-purple-100 text-purple-800',
    'iam:UpdateUser': 'bg-purple-50 text-purple-700',
    'iam:CreateRole': 'bg-purple-100 text-purple-800',
    'iam:DeleteRole': 'bg-purple-100 text-purple-800',
    'iam:UpdateRole': 'bg-purple-50 text-purple-700',
    'iam:AttachUserPolicy': 'bg-purple-100 text-purple-800',
    'iam:DetachUserPolicy': 'bg-purple-100 text-purple-800',
    'iam:AttachRolePolicy': 'bg-purple-100 text-purple-800',
    'iam:DetachRolePolicy': 'bg-purple-100 text-purple-800',
    'iam:CreateAccessKey': 'bg-purple-100 text-purple-800',
    'iam:DeleteAccessKey': 'bg-purple-100 text-purple-800'
  };
  return typeClasses[type] || 'bg-gray-100 text-gray-800';
};

// 获取风险级别名称
const getRiskLevelName = (level) => {
  const levelNames = {
    'low': t('audit.low'),
    'medium': t('audit.medium'),
    'high': t('audit.high')
  };
  return levelNames[level] || level;
};

// 获取风险级别样式
const getRiskLevelClass = (level) => {
  const levelClasses = {
    'low': 'bg-green-100 text-green-800',
    'medium': 'bg-yellow-100 text-yellow-800',
    'high': 'bg-red-100 text-red-800'
  };
  return levelClasses[level] || 'bg-gray-100 text-gray-800';
};

// 过滤审计记录
const filteredAudits = computed(() => {
  return auditsList.value.filter(audit => {
    // 类型过滤
    if (auditTypeFilter.value !== 'all' && audit.type !== auditTypeFilter.value) {
      return false;
    }

    // 风险级别过滤
    if (!riskLevelFilters.value.includes(audit.riskLevel)) {
      return false;
    }

    // 关键词搜索
    if (searchKeyword.value) {
      const keyword = searchKeyword.value.toLowerCase();
      const matchesKeyword =
        (audit.actor && audit.actor.toLowerCase().includes(keyword)) ||
        (audit.target && audit.target.toLowerCase().includes(keyword)) ||
        audit.description.toLowerCase().includes(keyword) ||
        (audit.ipAddress && audit.ipAddress.includes(keyword));
      if (!matchesKeyword) {
        return false;
      }
    }

    // 时间范围过滤
    if (startDate.value) {
      const start = new Date(startDate.value);
      if (audit.timestamp < start) {
        return false;
      }
    }

    if (endDate.value) {
      const end = new Date(endDate.value);
      // 结束时间设置为当天的23:59:59
      end.setHours(23, 59, 59, 999);
      if (audit.timestamp > end) {
        return false;
      }
    }

    return true;
  });
});

// 计算分页后的审计记录
const paginatedAudits = computed(() => {
  const startIndex = (currentPage.value - 1) * pageSize.value;
  const endIndex = startIndex + pageSize.value;
  return filteredAudits.value.slice(startIndex, endIndex);
});

// 计算总页数
const totalPages = computed(() => {
  return Math.ceil(filteredAudits.value.length / pageSize.value);
});

// 切换到指定页码
const changePage = (page) => {
  if (page >= 1 && page <= totalPages.value) {
    currentPage.value = page;
  }
};

// 切换每页显示数量
const changePageSize = (size) => {
  pageSize.value = size;
  currentPage.value = 1; // 重置到第一页
};

// 显示审计详情
const showAuditDetails = (audit) => {
  currentAudit.value = audit;
  detailsVisible.value = true;
};

// 关闭详情对话框
const closeDetails = () => {
  detailsVisible.value = false;
  currentAudit.value = {};
};

// 生成审计报告
const generateReport = (audit) => {
  showToastMessage(`审计报告已生成: ${audit.id}`, 'success');
};

// 导出审计记录
const exportAudits = () => {
  showToastMessage(`已导出 ${filteredAudits.value.length} 条审计记录`, 'success');
};

// 显示提示消息
const showToastMessage = (message, type = 'success') => {
  toastMessage.value = message;
  toastType.value = type;
  showToast.value = true;

  // 3秒后自动隐藏
  setTimeout(() => {
    showToast.value = false;
  }, 3000);
};

// 计算显示的页码
const displayPages = computed(() => {
  const pages = [];
  const total = totalPages.value;
  const current = currentPage.value;

  // 总是显示第一页
  pages.push(1);

  // 如果当前页离第一页超过2页，显示省略号
  if (current > 4) {
    pages.push('...');
  }

  // 显示当前页附近的页码
  const start = Math.max(2, current - 1);
  const end = Math.min(total - 1, current + 1);

  for (let i = start; i <= end; i++) {
    if (!pages.includes(i)) {
      pages.push(i);
    }
  }

  // 如果最后一页离当前页超过2页，显示省略号
  if (current < total - 3) {
    pages.push('...');
  }

  // 总是显示最后一页（如果不是第一页）
  if (total > 1) {
    if (!pages.includes(total)) {
      pages.push(total);
    }
  }

  return pages;
});

// 组件挂载时加载数据
onMounted(() => {
  loadMockData();
});
</script>

<style scoped>
.audits-container {
  max-width: 1200px;
  margin: 0 auto;
  padding: 2rem;
}

.page-header {
  margin-bottom: 1.5rem;
}

.search-container {
  margin-bottom: 1.5rem;
}

.filter-row {
  margin-bottom: 1.5rem;
}

.card {
  background: white;
  border-radius: 0.75rem;
  box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
  overflow: hidden;
}

/* 分页控件样式 */
.pagination {
  display: flex;
  align-items: center;
  justify-content: center;
  margin-top: 1rem;
}

.pagination button {
  display: flex;
  align-items: center;
  justify-content: center;
  min-width: 2rem;
  height: 2rem;
  padding: 0 0.5rem;
  border: 1px solid #e2e8f0;
  background-color: white;
  color: #64748b;
  font-size: 0.875rem;
  cursor: pointer;
  transition: all 0.2s ease;
}

.pagination button:hover:not(:disabled) {
  background-color: #f8fafc;
  border-color: #cbd5e1;
}

.pagination button:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

.pagination button.active {
  background-color: #3b82f6;
  border-color: #3b82f6;
  color: white;
}

.pagination select {
  padding: 0.25rem 0.5rem;
  border: 1px solid #e2e8f0;
  border-radius: 0.375rem;
  background-color: white;
  font-size: 0.875rem;
}

/* 动画效果 */
@keyframes fadeIn {
  from {
    opacity: 0;
    transform: translateY(20px);
  }

  to {
    opacity: 1;
    transform: translateY(0);
  }
}

.animate-fadeIn {
  animation: fadeIn 0.3s ease-out;
}

/* 响应式设计 */
@media (max-width: 768px) {
  .audits-container {
    padding: 1rem;
  }

  .page-header {
    flex-direction: column;
    align-items: flex-start;
    gap: 1rem;
  }

  .filter-row {
    flex-direction: column;
    align-items: flex-start;
  }

  .card {
    margin: -1rem;
    border-radius: 0;
  }

  table {
    font-size: 0.875rem;
  }

  th,
  td {
    padding: 0.75rem !important;
  }

  .max-w-xs {
    max-width: none;
  }
}
</style>