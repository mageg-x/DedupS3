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
            <!-- 控制台基本操作 -->
            <option value="console:Login">{{ t('auditTypes.console:Login') }}</option>
            <option value="console:Logout">{{ t('auditTypes.console:Logout') }}</option>
            <option value="console:GetStats">{{ t('auditTypes.console:GetStats') }}</option>
            <!-- 存储桶操作 -->
            <option value="console:ListBuckets">{{ t('auditTypes.console:ListBuckets') }}</option>
            <option value="console:CreateBucket">{{ t('auditTypes.console:CreateBucket') }}</option>
            <option value="console:DeleteBucket">{{ t('auditTypes.console:DeleteBucket') }}</option>
            <option value="console:ListObjects">{{ t('auditTypes.console:ListObjects') }}</option>
            <option value="console:CreateFolder">{{ t('auditTypes.console:CreateFolder') }}</option>
            <option value="console:PutObject">{{ t('auditTypes.console:PutObject') }}</option>
            <option value="console:DeleteObject">{{ t('auditTypes.console:DeleteObject') }}</option>
            <option value="console:GetObject">{{ t('auditTypes.console:GetObject') }}</option>
            <!-- 用户管理 -->
            <option value="console:GetUserInfo">{{ t('auditTypes.console:GetUserInfo') }}</option>
            <option value="console:ListUsers">{{ t('auditTypes.console:ListUsers') }}</option>
            <option value="console:CreateUser">{{ t('auditTypes.console:CreateUser') }}</option>
            <option value="console:UpdateUser">{{ t('auditTypes.console:UpdateUser') }}</option>
            <option value="console:DeleteUser">{{ t('auditTypes.console:DeleteUser') }}</option>
            <!-- 用户组管理 -->
            <option value="console:ListGroups">{{ t('auditTypes.console:ListGroups') }}</option>
            <option value="console:GetGroup">{{ t('auditTypes.console:GetGroup') }}</option>
            <option value="console:CreateGroup">{{ t('auditTypes.console:CreateGroup') }}</option>
            <option value="console:UpdateGroup">{{ t('auditTypes.console:UpdateGroup') }}</option>
            <option value="console:DeleteGroup">{{ t('auditTypes.console:DeleteGroup') }}</option>
            <!-- 角色管理 -->
            <option value="console:ListRoles">{{ t('auditTypes.console:ListRoles') }}</option>
            <option value="console:GetRole">{{ t('auditTypes.console:GetRole') }}</option>
            <option value="console:CreateRole">{{ t('auditTypes.console:CreateRole') }}</option>
            <option value="console:UpdateRole">{{ t('auditTypes.console:UpdateRole') }}</option>
            <option value="console:DeleteRole">{{ t('auditTypes.console:DeleteRole') }}</option>
            <!-- 策略管理 -->
            <option value="console:ListPolicies">{{ t('auditTypes.console:ListPolicies') }}</option>
            <option value="console:GetPolicy">{{ t('auditTypes.console:GetPolicy') }}</option>
            <option value="console:CreatePolicy">{{ t('auditTypes.console:CreatePolicy') }}</option>
            <option value="console:UpdatePolicy">{{ t('auditTypes.console:UpdatePolicy') }}</option>
            <option value="console:DeletePolicy">{{ t('auditTypes.console:DeletePolicy') }}</option>
            <!-- 访问密钥管理 -->
            <option value="console:ListAccessKeys">{{ t('auditTypes.console:ListAccessKeys') }}</option>
            <option value="console:CreateAccessKey">{{ t('auditTypes.console:CreateAccessKey') }}</option>
            <option value="console:UpdateAccessKey">{{ t('auditTypes.console:UpdateAccessKey') }}</option>
            <option value="console:DeleteAccessKey">{{ t('auditTypes.console:DeleteAccessKey') }}</option>
            <!-- 配额管理 -->
            <option value="console:ListQuotas">{{ t('auditTypes.console:ListQuotas') }}</option>
            <option value="console:CreateQuota">{{ t('auditTypes.console:CreateQuota') }}</option>
            <option value="console:UpdateQuota">{{ t('auditTypes.console:UpdateQuota') }}</option>
            <option value="console:DeleteQuota">{{ t('auditTypes.console:DeleteQuota') }}</option>
            <!-- 分块配置管理 -->
            <option value="console:ListChunkConfigs">{{ t('auditTypes.console:ListChunkConfigs') }}</option>
            <option value="console:GetChunkConfig">{{ t('auditTypes.console:GetChunkConfig') }}</option>
            <option value="console:UpdateChunkConfig">{{ t('auditTypes.console:UpdateChunkConfig') }}</option>
            <!-- 存储管理 -->
            <option value="console:ListStorages">{{ t('auditTypes.console:ListStorages') }}</option>
            <option value="console:CreateStorage">{{ t('auditTypes.console:CreateStorage') }}</option>
            <option value="console:TestStorage">{{ t('auditTypes.console:TestStorage') }}</option>
            <option value="console:DeleteStorage">{{ t('auditTypes.console:DeleteStorage') }}</option>
            <!-- 调试操作 -->
            <option value="console:DebugObjectInfo">{{ t('auditTypes.console:DebugObjectInfo') }}</option>
            <option value="console:DebugBlockInfo">{{ t('auditTypes.console:DebugBlockInfo') }}</option>
            <option value="console:DebugChunkInfo">{{ t('auditTypes.console:DebugChunkInfo') }}</option>
            <!-- 日志查看 -->
            <option value="console:ListAuditLog">{{ t('auditTypes.console:ListAuditLog') }}</option>
            <option value="console:ListEventLog">{{ t('auditTypes.console:ListEventLog') }}</option>
            <!-- S3 操作 - 多部分上传 -->
            <option value="s3:AbortMultipartUpload">{{ t('auditTypes.s3:AbortMultipartUpload') }}</option>
            <option value="s3:CompleteMultipartUpload">{{ t('auditTypes.s3:CompleteMultipartUpload') }}</option>
            <option value="s3:CreateMultipartUpload">{{ t('auditTypes.s3:CreateMultipartUpload') }}</option>
            <option value="s3:ListMultipartUploads">{{ t('auditTypes.s3:ListMultipartUploads') }}</option>
            <option value="s3:ListParts">{{ t('auditTypes.s3:ListParts') }}</option>
            <option value="s3:UploadPartCopy">{{ t('auditTypes.s3:UploadPartCopy') }}</option>
            <option value="s3:UploadPart">{{ t('auditTypes.s3:UploadPart') }}</option>
            <!-- S3 操作 - 对象操作 -->
            <option value="s3:CopyObject">{{ t('auditTypes.s3:CopyObject') }}</option>
            <option value="s3:DeleteObject">{{ t('auditTypes.s3:DeleteObject') }}</option>
            <option value="s3:DeleteObjects">{{ t('auditTypes.s3:DeleteObjects') }}</option>
            <option value="s3:GetObject">{{ t('auditTypes.s3:GetObject') }}</option>
            <option value="s3:HeadObject">{{ t('auditTypes.s3:HeadObject') }}</option>
            <option value="s3:PutObject">{{ t('auditTypes.s3:PutObject') }}</option>
            <option value="s3:RenameObject">{{ t('auditTypes.s3:RenameObject') }}</option>
            <option value="s3:RestoreObject">{{ t('auditTypes.s3:RestoreObject') }}</option>
            <option value="s3:SelectObjectContent">{{ t('auditTypes.s3:SelectObjectContent') }}</option>
            <!-- S3 操作 - 对象属性和标签 -->
            <option value="s3:DeleteObjectTagging">{{ t('auditTypes.s3:DeleteObjectTagging') }}</option>
            <option value="s3:GetObjectAcl">{{ t('auditTypes.s3:GetObjectAcl') }}</option>
            <option value="s3:GetObjectAttributes">{{ t('auditTypes.s3:GetObjectAttributes') }}</option>
            <option value="s3:GetObjectLegalHold">{{ t('auditTypes.s3:GetObjectLegalHold') }}</option>
            <option value="s3:GetObjectRetention">{{ t('auditTypes.s3:GetObjectRetention') }}</option>
            <option value="s3:GetObjectTagging">{{ t('auditTypes.s3:GetObjectTagging') }}</option>
            <option value="s3:GetObjectWithLambda">{{ t('auditTypes.s3:GetObjectWithLambda') }}</option>
            <option value="s3:PutObjectAcl">{{ t('auditTypes.s3:PutObjectAcl') }}</option>
            <option value="s3:PutObjectExtract">{{ t('auditTypes.s3:PutObjectExtract') }}</option>
            <option value="s3:PutObjectLegalHold">{{ t('auditTypes.s3:PutObjectLegalHold') }}</option>
            <option value="s3:PutObjectRetention">{{ t('auditTypes.s3:PutObjectRetention') }}</option>
            <option value="s3:PutObjectTagging">{{ t('auditTypes.s3:PutObjectTagging') }}</option>
            <!-- S3 操作 - 存储桶操作 -->
            <option value="s3:CreateBucket">{{ t('auditTypes.s3:CreateBucket') }}</option>
            <option value="s3:DeleteBucket">{{ t('auditTypes.s3:DeleteBucket') }}</option>
            <option value="s3:HeadBucket">{{ t('auditTypes.s3:HeadBucket') }}</option>
            <option value="s3:ListBuckets">{{ t('auditTypes.s3:ListBuckets') }}</option>
            <option value="s3:ListObjects">{{ t('auditTypes.s3:ListObjects') }}</option>
            <option value="s3:ListObjectsV2">{{ t('auditTypes.s3:ListObjectsV2') }}</option>
            <option value="s3:ListObjectsV2WithMetadata">{{ t('auditTypes.s3:ListObjectsV2WithMetadata') }}</option>
            <!-- S3 操作 - 存储桶配置 -->
            <option value="s3:DeleteBucketCors">{{ t('auditTypes.s3:DeleteBucketCors') }}</option>
            <option value="s3:DeleteBucketEncryption">{{ t('auditTypes.s3:DeleteBucketEncryption') }}</option>
            <option value="s3:DeleteBucketLifecycleConfiguration">{{
              t('auditTypes.s3:DeleteBucketLifecycleConfiguration') }}</option>
            <option value="s3:DeleteBucketTagging">{{ t('auditTypes.s3:DeleteBucketTagging') }}</option>
            <option value="s3:DeleteBucketWebsite">{{ t('auditTypes.s3:DeleteBucketWebsite') }}</option>
            <option value="s3:GetBucketAccelerateConfiguration">{{ t('auditTypes.s3:GetBucketAccelerateConfiguration')
              }}</option>
            <option value="s3:GetBucketAcl">{{ t('auditTypes.s3:GetBucketAcl') }}</option>
            <option value="s3:GetBucketCors">{{ t('auditTypes.s3:GetBucketCors') }}</option>
            <option value="s3:GetBucketEncryption">{{ t('auditTypes.s3:GetBucketEncryption') }}</option>
            <option value="s3:GetBucketLifecycleConfiguration">{{ t('auditTypes.s3:GetBucketLifecycleConfiguration') }}
            </option>
            <option value="s3:GetBucketLocation">{{ t('auditTypes.s3:GetBucketLocation') }}</option>
            <option value="s3:GetBucketLogging">{{ t('auditTypes.s3:GetBucketLogging') }}</option>
            <option value="s3:GetBucketNotificationConfiguration">{{
              t('auditTypes.s3:GetBucketNotificationConfiguration') }}</option>
            <option value="s3:GetBucketObjectLockConfiguration">{{ t('auditTypes.s3:GetBucketObjectLockConfiguration')
              }}</option>
            <option value="s3:GetBucketPolicy">{{ t('auditTypes.s3:GetBucketPolicy') }}</option>
            <option value="s3:GetBucketPolicyStatus">{{ t('auditTypes.s3:GetBucketPolicyStatus') }}</option>
            <option value="s3:GetBucketReplication">{{ t('auditTypes.s3:GetBucketReplication') }}</option>
            <option value="s3:GetBucketRequestPayment">{{ t('auditTypes.s3:GetBucketRequestPayment') }}</option>
            <option value="s3:GetBucketTagging">{{ t('auditTypes.s3:GetBucketTagging') }}</option>
            <option value="s3:GetBucketVersioning">{{ t('auditTypes.s3:GetBucketVersioning') }}</option>
            <option value="s3:GetBucketWebsite">{{ t('auditTypes.s3:GetBucketWebsite') }}</option>
            <option value="s3:PutBucketAcl">{{ t('auditTypes.s3:PutBucketAcl') }}</option>
            <option value="s3:PutBucketCors">{{ t('auditTypes.s3:PutBucketCors') }}</option>
            <option value="s3:PutBucketEncryption">{{ t('auditTypes.s3:PutBucketEncryption') }}</option>
            <option value="s3:PutBucketLifecycleConfiguration">{{ t('auditTypes.s3:PutBucketLifecycleConfiguration') }}
            </option>
            <option value="s3:PutBucketNotificationConfiguration">{{
              t('auditTypes.s3:PutBucketNotificationConfiguration') }}</option>
            <option value="s3:PutBucketObjectLockConfiguration">{{ t('auditTypes.s3:PutBucketObjectLockConfiguration')
              }}</option>
            <option value="s3:PutBucketPolicy">{{ t('auditTypes.s3:PutBucketPolicy') }}</option>
            <option value="s3:PutBucketReplication">{{ t('auditTypes.s3:PutBucketReplication') }}</option>
            <option value="s3:PutBucketTagging">{{ t('auditTypes.s3:PutBucketTagging') }}</option>
            <option value="s3:PutBucketVersioning">{{ t('auditTypes.s3:PutBucketVersioning') }}</option>
            <!-- S3 操作 - 版本控制 -->
            <option value="s3:ListObjectVersions">{{ t('auditTypes.s3:ListObjectVersions') }}</option>
            <option value="s3:ListObjectVersionsWithMetadata">{{ t('auditTypes.s3:ListObjectVersionsWithMetadata') }}
            </option>
          </select>
          <div class="pointer-events-none absolute inset-y-0 right-0 flex items-center px-2 text-gray-500">
            <i class="fas fa-chevron-down text-xs"></i>
          </div>
        </div>
        <button @click="exportAudits"
          class="flex items-center gap-1 px-3 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors">
          <i class="fas fa-download"></i>
          <span>{{ t('common.export') }}</span>
        </button>
      </div>
    </div>

    <!-- 搜索框 -->
    <div class="search-container mb-6 text-sm">
      <div class="relative">
        <input type="text" v-model="searchKeyword" :placeholder="t('audit.searchPlaceholder')"
          class="text-sm w-full px-4 py-2 pl-10 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent transition-all" />
        <i class="fas fa-search absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400"></i>
      </div>
    </div>

    <!-- 时间范围选择和风险级别筛选 -->
    <div class="filter-row mb-6 flex flex-wrap gap-2 items-center">
      <div class="flex items-center">
        <label class="text-sm font-medium text-gray-700 mr-1">{{ t('audit.startTime') }}</label>
        <el-date-picker v-model="startDate" type="datetime" format="YYYY-MM-DD HH:mm:ss"
          value-format="YYYY-MM-DDTHH:mm:ss" :placeholder="t('common.selectStartTime')" class="w-full text-[0.8rem]" />
      </div>
      <div class="flex items-center">
        <label class="text-sm font-medium text-gray-700 mr-1">{{ t('audit.endTime') }}</label>
        <el-date-picker v-model="endDate" type="datetime" format="YYYY-MM-DD HH:mm:ss"
          value-format="YYYY-MM-DDTHH:mm:ss" :placeholder="t('common.selectEndTime')" class="w-full text-[0.8rem]" />
      </div>
      <div class="flex items-center">
        <label class="text-sm font-medium text-gray-700 mr-2">{{ t('audit.riskLevel') }}</label>
        <div class="flex gap-2">
          <label class="inline-flex items-center">
            <input type="checkbox" v-model="riskLevelFilters" value="low"
              class="form-checkbox h-4 w-4 text-blue-600 rounded border-gray-300 focus:ring-blue-500" />
            <span class="ml-2 text-sm text-gray-700">{{ t('audit.low') }}</span>
          </label>
          <label class="inline-flex items-center">
            <input type="checkbox" v-model="riskLevelFilters" value="medium"
              class="form-checkbox h-4 w-4 text-blue-600 rounded border-gray-300 focus:ring-blue-500" />
            <span class="ml-2 text-sm text-gray-700">{{ t('audit.medium') }}</span>
          </label>
          <label class="inline-flex items-center">
            <input type="checkbox" v-model="riskLevelFilters" value="high"
              class="form-checkbox h-4 w-4 text-blue-600 rounded border-gray-300 focus:ring-blue-500" />
            <span class="ml-2 text-sm text-gray-700">{{ t('audit.high') }}</span>
          </label>
        </div>
      </div>
    </div>

    <!-- 审计记录列表 -->
    <div class="card bg-white rounded-xl shadow-sm overflow-hidden">
      <div class="overflow-x-auto">
        <table class="w-full fixed-table">
          <thead class="bg-gray-50 border-b border-gray-200">
            <tr>
              <th class="px-6 py-3 text-left text-sm font-medium text-gray-500 uppercase tracking-wider w-32">
                {{ t('audit.time') }}
              </th>
              <th class="px-6 py-3 text-left text-sm font-medium text-gray-500 uppercase tracking-wider w-36">
                {{ t('audit.auditType') }}
              </th>
              <th class="px-6 py-3 text-left text-sm font-medium text-gray-500 uppercase tracking-wider w-28">
                {{ t('audit.actor') }}
              </th>
              <th class="px-6 py-3 text-left text-sm font-medium text-gray-500 uppercase tracking-wider">
                {{ t('audit.target') }}
              </th>
              <th class="px-6 py-3 text-left text-sm font-medium text-gray-500 uppercase tracking-wider w-36">
                {{ t('audit.riskLevel') }}
              </th>
              <th class="px-6 py-3 text-left text-sm font-medium text-gray-500 uppercase tracking-wider w-20">
                {{ t('audit.status') }}
              </th>
              <th class="px-6 py-3 text-right text-sm font-medium text-gray-500 uppercase tracking-wider w-32">
                {{ t('audit.operation') }}
              </th>
            </tr>
          </thead>
          <tbody class="text-sm bg-white divide-y divide-gray-200">
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
              <td class="px-6 py-4 text-sm text-gray-700">
                {{ audit.target || 'N/A' }}
              </td>
              <td class="px-6 py-4 whitespace-nowrap min-w-40">
                <span :class="['px-2 py-1 text-xs rounded-full', getRiskLevelClass(audit.riskLevel)]">
                  {{ getRiskLevelName(audit.riskLevel) }}
                </span>
              </td>
              <td class="px-6 py-4 whitespace-nowrap">
                <span :class="[
                  'px-2 py-1 text-xs rounded-full',
                  audit.status === 'success' ? 'bg-green-100 text-green-800' : 'bg-red-100 text-red-800'
                ]">
                  {{ audit.status === 'success' ? t('common.success') : t('common.failed') }}
                </span>
              </td>
              <td class="px-6 py-4 whitespace-nowrap text-right text-sm font-medium">
                <button @click="showAuditDetails(audit)"
                  class="text-blue-600 hover:text-blue-900 transition-colors mr-3">
                  <i class="fas fa-eye mr-1"></i>{{ t('common.details') }}
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
    <div v-if="filteredAudits.length > 0" class="flex items-center justify-between mt-4">
      <div class="flex items-center space-x-2">
        <span class="text-sm text-gray-600">
          {{ t('common.totalRecords', { total: filteredAudits.length }) }}
        </span>
        <select v-model="pageSize" @change="changePageSize" class="border border-gray-300 rounded-md text-sm">
          <option value="10">10 条/页</option>
          <option value="20">20 条/页</option>
          <option value="50">50 条/页</option>
        </select>
      </div>
      <div class="flex items-center space-x-1">
        <button @click="changePage(currentPage - 1)" :disabled="currentPage === 1"
          class="px-3 py-1 text-sm border rounded-md hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed">
          <i class="fas fa-chevron-left"></i>
        </button>
        <button v-for="page in totalPages" :key="page" @click="changePage(page)"
          class="px-3 py-1 text-sm rounded-md transition-colors"
          :class="currentPage === page ? 'bg-blue-100 text-blue-800' : 'hover:bg-gray-100'">
          {{ page }}
        </button>
        <button @click="changePage(currentPage + 1)" :disabled="!hasMore && currentPage === totalPages"
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
              <span class="font-medium text-blue-600 hover:text-blue-800 cursor-pointer truncate max-w-[50%]">
                {{ currentAudit.target || 'N/A' }}
              </span>
            </div>
            <div class="flex justify-between">
              <span class="text-gray-500">{{ t('audit.riskLevel') }}:</span>
              <span :class="['px-2 py-1 text-xs rounded-full', getRiskLevelClass(currentAudit.riskLevel)]">
                {{ getRiskLevelName(currentAudit.riskLevel) }}
              </span>
            </div>
            <div class="flex justify-between">
              <span class="text-gray-500">{{ t('audit.requestStatus') }}:</span>
              <span :class="[
                'px-2 py-1 text-xs rounded-full',
                currentAudit.status === 'success' ? 'bg-green-100 text-green-800' : 'bg-red-100 text-red-800'
              ]">
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
    <div v-if="showToast" :class="[
      'fixed top-4 right-4 px-4 py-3 rounded-lg shadow-lg transition-all duration-300 z-50',
      toastType === 'success' ? 'bg-green-500 text-white' : 'bg-red-500 text-white'
    ]">
      <div class="flex items-center gap-2">
        <i v-if="toastType === 'success'" class="fas fa-check-circle"></i>
        <i v-else class="fas fa-exclamation-circle"></i>
        <span>{{ toastMessage }}</span>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, watch } from 'vue';
import { useI18n } from 'vue-i18n';
import { ElDatePicker } from 'element-plus';
import { listauditlog } from '@/api/admin.js';

const { t } = useI18n();

// ======================
// 响应式状态 - 数据
// ======================
const auditsList = ref([]);
const currentAudit = ref({});
const hasMore = ref(false);

// ======================
// 响应式状态 - 筛选与搜索
// ======================
const searchKeyword = ref('');
const auditTypeFilter = ref('all');
const startDate = ref('');
const endDate = ref('');
const riskLevelFilters = ref(['low', 'medium', 'high']);

// ======================
// 响应式状态 - 分页
// ======================
const currentPage = ref(1);
const pageSize = ref(10);

// ======================
// 响应式状态 - UI 控制
// ======================
const detailsVisible = ref(false);
const showToast = ref(false);
const toastMessage = ref('');
const toastType = ref('success');

// ======================
// 计算属性
// ======================
const filteredAudits = computed(() => {
  return auditsList.value.filter((audit) => {
    const matchesSearch =
      !searchKeyword.value ||
      (audit.timestamp && audit.timestamp.includes(searchKeyword.value)) ||
      (audit.type && audit.type.includes(searchKeyword.value)) ||
      (audit.actor && audit.actor.includes(searchKeyword.value)) ||
      (audit.target && audit.target.includes(searchKeyword.value));

    const matchesType = auditTypeFilter.value === 'all' || audit.type === auditTypeFilter.value;

    const matchesRisk =
      riskLevelFilters.value.length === 0 || riskLevelFilters.value.includes(audit.riskLevel);

    let matchesTime = true;
    if (startDate.value || endDate.value) {
      const ts = new Date(audit.timestamp).getTime();
      const start = startDate.value ? new Date(startDate.value).getTime() : -Infinity;
      const end = endDate.value ? new Date(endDate.value).getTime() : Infinity;
      matchesTime = ts >= start && ts <= end;
    }

    return matchesSearch && matchesType && matchesRisk && matchesTime;
  });
});

const totalPages = computed(() => {
  return Math.ceil(filteredAudits.value.length / pageSize.value) || 1;
});

const paginatedAudits = computed(() => {
  const start = (currentPage.value - 1) * pageSize.value;
  return filteredAudits.value.slice(start, start + pageSize.value);
});

// ======================
// 工具函数：映射 API 数据
// ======================
const mapApiDataToFrontend = (apiData) => {
  const actor =
    apiData.userIdentity?.userName ||
    apiData.userIdentity?.principalId ||
    t('common.system');

  // 简单风险等级推断（可根据实际业务调整）
  const eventName = apiData.eventName || '';
  let riskLevel = 'low';
  if (
    eventName.includes('Delete') 
  ) {
    riskLevel = 'high';
  } else if (
    (eventName.includes('Update') || eventName.includes('Create')) &&
    ( eventName.includes('Group') ||
    eventName.includes('Policy') ||
    eventName.includes('AccessKey') ||
    eventName.includes('Role') ||
    eventName.includes('User'))
  ) {
    riskLevel = 'medium';
  }

  return {
    id: apiData.eventID || apiData.id,
    timestamp: apiData.eventTime,
    type: apiData.eventName,
    actor: actor,
    target: apiData.resources?.[0]?.ARN || apiData.bucketName || apiData.key || 'N/A',
    riskLevel: riskLevel,
    status: apiData.errorCode === "0" ? 'success' : 'failure',
    errorCode: apiData.errorCode || '',
    errorMessage: apiData.errorMessage || '',
    description: apiData.eventMessage || apiData.requestParameters?.toString() || '',
    ipAddress: apiData.sourceIPAddress || '',
    context: apiData
  };
};

// ======================
// 数据加载
// ======================
const loadAuditData = async () => {
  try {
    let startTime, endTime;

    if (startDate.value && endDate.value) {
      startTime = Math.floor(new Date(startDate.value).getTime() / 1000);
      endTime = Math.floor(new Date(endDate.value).getTime() / 1000);
    } else {
      const now = new Date();
      endTime = Math.floor(now.getTime() / 1000);
      startTime = Math.floor((now.getTime() - 7 * 24 * 60 * 60 * 1000) / 1000);
    }

    const params = {
      startTime: startTime.toString(),
      endTime: endTime.toString(),
      eventName: auditTypeFilter.value === 'all' ? '' : auditTypeFilter.value,
      offset: ((currentPage.value - 1) * pageSize.value).toString(),
      limit: pageSize.value.toString()
    };

    const response = await listauditlog(params);
    if (response.code === 0) {
      auditsList.value = response.data.entries.map(mapApiDataToFrontend);
      // 处理hasMore字段
      hasMore.value = response.data.hasMore || false;
    } else {
      showToastMessage(`加载审计数据失败: ${response.msg}`, 'error');
    }
  } catch (error) {
    console.error('加载审计数据错误:', error);
    showToastMessage('加载审计数据失败', 'error');
  }
};

// ======================
// UI 交互函数
// ======================
const showAuditDetails = (audit) => {
  currentAudit.value = audit;
  detailsVisible.value = true;
};

const closeDetails = () => {
  detailsVisible.value = false;
  currentAudit.value = {};
};

const exportAudits = () => {
  showToastMessage('导出功能暂未实现', 'success');
};

const generateReport = (audit) => {
  showToastMessage(`已生成 ${audit.type} 的报告`, 'success');
};

// ======================
// 分页控制
// ======================
const changePage = (page) => {
  if (page < 1 || page > totalPages.value) return;
  currentPage.value = page;
};

const changePageSize = () => {
  currentPage.value = 1;
};

// ======================
// 提示消息
// ======================
const showToastMessage = (message, type = 'success') => {
  toastMessage.value = message;
  toastType.value = type;
  showToast.value = true;
  setTimeout(() => {
    showToast.value = false;
  }, 3000);
};

// ======================
// 格式化与展示辅助函数
// ======================
const formatDate = (timestamp) => {
  return new Date(timestamp).toLocaleString();
};

const getAuditTypeName = (type) => {
  return t(`auditTypes.${type}`) || type;
};

const getAuditTypeClass = (type) => {
  // 扩展的颜色调色板（20+ 种 Tailwind 兼容的背景/文字组合）
  const colorPalettes = [
    'bg-blue-100 text-blue-800',
    'bg-green-100 text-green-800',
    'bg-purple-100 text-purple-800',
    'bg-yellow-100 text-yellow-800',
    'bg-pink-100 text-pink-800',
    'bg-orange-100 text-orange-800',
    'bg-cyan-100 text-cyan-800',
    'bg-indigo-100 text-indigo-800',
    'bg-emerald-100 text-emerald-800',
    'bg-amber-100 text-amber-800',
    'bg-red-100 text-red-800',
    'bg-violet-100 text-violet-800',
    'bg-fuchsia-100 text-fuchsia-800',
    'bg-sky-100 text-sky-800',
    'bg-teal-100 text-teal-800',
    'bg-lime-100 text-lime-800',
    'bg-rose-100 text-rose-800',
    'bg-stone-100 text-stone-800',
    'bg-zinc-100 text-zinc-800',
    'bg-slate-100 text-slate-800',
    'bg-gray-100 text-gray-800',
  ];

  // 简单、稳定的字符串哈希函数（djb2 变种）
  const hashString = (str) => {
    let hash = 5381;
    for (let i = 0; i < str.length; i++) {
      hash = (hash * 33) ^ str.charCodeAt(i);
    }
    return Math.abs(hash >>> 0); // 转为无符号32位正整数
  };

  if (!type || typeof type !== 'string') {
    return colorPalettes[colorPalettes.length - 1]; // 默认灰色
  }

  const hash = hashString(type);
  const index = hash % colorPalettes.length;
  return colorPalettes[index];
};

const getRiskLevelName = (level) => {
  return t(`audit.${level}`) || level;
};

const getRiskLevelClass = (level) => {
  switch (level) {
    case 'high':
      return 'bg-red-100 text-red-800';
    case 'medium':
      return 'bg-yellow-100 text-yellow-800';
    case 'low':
      return 'bg-green-100 text-green-800';
    default:
      return 'bg-gray-100 text-gray-800';
  }
};

// ======================
// 生命周期与监听
// ======================
onMounted(() => {
  loadAuditData();
});

watch([searchKeyword, auditTypeFilter, riskLevelFilters, startDate, endDate], () => {
  currentPage.value = 1;
});

watch([currentPage, pageSize], () => {
  // 当前为前端分页，无需重新请求
  // 若需后端分页，请在此调用 loadAuditData()
});
</script>

<style scoped>
.audits-container {
  max-width: 1200px;
  margin: 0 auto;
  padding: 2rem 1rem;
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

.fixed-table {
  /* 关键：固定表格布局 */
  width: 100%;
  /* 或指定具体宽度 */
  border-collapse: collapse;
}

.fixed-table th,
.fixed-table td {
  /* 可为每列单独设置 */
  max-width: 300px;
  /* 限制最大宽度 */
  overflow: hidden;
  /* 隐藏溢出内容 */
  text-overflow: ellipsis;
  /* 显示省略号 */
  white-space: nowrap;
  /* 禁止换行 */
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