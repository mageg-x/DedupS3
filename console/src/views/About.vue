<template>
  <div class="about-container">
    <!-- 头部搜索和标题 -->
    <div class="about-header flex items-center justify-between mb-6">
      <h2 class="text-2xl font-bold text-gray-800">{{ $t('about.pageTitle') }}</h2>
    </div>

    <!-- 介绍内容区域 -->
    <div class="about-content bg-white rounded-xl shadow-md p-6">
      <!-- 这里直接嵌入 about.html 的内容 -->
      <div class="about-inner">
        <header class="about-page-header">
          <div class="container header-content">
            <h1>Boulder</h1>
            <p class="subtitle">{{ $t('about.subtitle') }}</p>
            <p>{{ $t('about.description') }}</p>
            <span class="tagline">{{ $t('about.tagline') }}</span>
          </div>
        </header>

        <!-- Navigation -->
        <nav class="about-navigation">
          <div class="container nav-container">
            <a href="#features" class="nav-link active">{{ $t('about.navFeatures') }}</a>
            <a href="#architecture" class="nav-link">{{ $t('about.navArchitecture') }}</a>
            <a href="#plugins" class="nav-link">{{ $t('about.navPlugins') }}</a>
            <a href="#api" class="nav-link">{{ $t('about.navAPI') }}</a>
            <a href="#performance" class="nav-link">{{ $t('about.navPerformance') }}</a>
            <a href="#license" class="nav-link">{{ $t('about.navLicense') }}</a>
          </div>
        </nav>

        <!-- Core Features -->
        <section id="features" class="about-section">
          <div class="container">
            <h2 class="section-title">{{ $t('about.featuresTitle') }}</h2>
            <div class="features-grid">
              <div class="feature-card">
                <div class="feature-icon">📊</div>
                <h3 class="feature-title">{{ $t('about.feature1Title') }}</h3>
                <p class="feature-desc">{{ $t('about.feature1Desc') }}</p>
              </div>
              <div class="feature-card">
                <div class="feature-icon">🔄</div>
                <h3 class="feature-title">{{ $t('about.feature2Title') }}</h3>
                <p class="feature-desc">{{ $t('about.feature2Desc') }}</p>
              </div>
              <div class="feature-card">
                <div class="feature-icon">⚡</div>
                <h3 class="feature-title">{{ $t('about.feature3Title') }}</h3>
                <p class="feature-desc">{{ $t('about.feature3Desc') }}</p>
              </div>
              <div class="feature-card">
                <div class="feature-icon">🌐</div>
                <h3 class="feature-title">{{ $t('about.feature4Title') }}</h3>
                <p class="feature-desc">{{ $t('about.feature4Desc') }}</p>
              </div>
              <div class="feature-card">
                <div class="feature-icon">🏗️</div>
                <h3 class="feature-title">{{ $t('about.feature5Title') }}</h3>
                <p class="feature-desc">{{ $t('about.feature5Desc') }}</p>
              </div>
              <div class="feature-card">
                <div class="feature-icon">🔒</div>
                <h3 class="feature-title">{{ $t('about.feature6Title') }}</h3>
                <p class="feature-desc">{{ $t('about.feature6Desc') }}</p>
              </div>
            </div>
          </div>
        </section>

        <!-- Technical Architecture -->
        <section id="architecture" class="about-section" style="background: var(--gray-100);">
          <div class="container">
            <h2 class="section-title">{{ $t('about.architectureTitle') }}</h2>
            <div class="architecture">
              <h3>{{ $t('about.architectureThreeLevels') }}</h3>
              <p>{{ $t('about.architectureDesc') }}</p>

              <div class="architecture-diagram">
                <img :src="architectureImagePath" :alt="$t('about.architectureDiagramAlt')" class="architecture-image">
              </div>

              <div class="features-grid">
                <div class="feature-card">
                  <h3 class="feature-title">{{ $t('about.architectureObjectLayer') }}</h3>
                  <p class="feature-desc">{{ $t('about.architectureObjectDesc') }}</p>
                </div>
                <div class="feature-card">
                  <h3 class="feature-title">{{ $t('about.architectureSliceLayer') }}</h3>
                  <p class="feature-desc">{{ $t('about.architectureSliceDesc') }}</p>
                </div>
                <div class="feature-card">
                  <h3 class="feature-title">{{ $t('about.architectureBlockLayer') }}</h3>
                  <p class="feature-desc">{{ $t('about.architectureBlockDesc') }}</p>
                </div>
              </div>
            </div>
          </div>
        </section>

        <!-- Plugin Architecture -->
        <section id="plugins" class="about-section">
          <div class="container">
            <h2 class="section-title">{{ $t('about.pluginsTitle') }}</h2>
            <p>{{ $t('about.pluginsDescription') }}</p>
            
            <div class="features-grid">
              <div class="feature-card">
                <div class="feature-icon">⚙️</div>
                <h3 class="feature-title">{{ $t('about.pluginConfigCenter') }}</h3>
                <p class="feature-desc">{{ $t('about.pluginConfigCenterDesc') }}</p>
              </div>
              <div class="feature-card">
                <div class="feature-icon">📝</div>
                <h3 class="feature-title">{{ $t('about.pluginEventLog') }}</h3>
                <p class="feature-desc">{{ $t('about.pluginEventLogDesc') }}</p>
              </div>
              <div class="feature-card">
                <div class="feature-icon">🔍</div>
                <h3 class="feature-title">{{ $t('about.pluginAuditLog') }}</h3>
                <p class="feature-desc">{{ $t('about.pluginAuditLogDesc') }}</p>
              </div>
              <div class="feature-card">
                <div class="feature-icon">💾</div>
                <h3 class="feature-title">{{ $t('about.pluginMetadataStore') }}</h3>
                <p class="feature-desc">{{ $t('about.pluginMetadataStoreDesc') }}</p>
              </div>
              <div class="feature-card">
                <div class="feature-icon">📦</div>
                <h3 class="feature-title">{{ $t('about.pluginBlockStore') }}</h3>
                <p class="feature-desc">{{ $t('about.pluginBlockStoreDesc') }}</p>
              </div>
            </div>
          </div>
        </section>

        <!-- API Support -->
        <section id="api" class="about-section">
          <div class="container">
            <h2 class="section-title">{{ $t('about.apiTitle') }}</h2>
            <table class="api-table">
              <thead>
                <tr>
                  <th>{{ $t('about.apiType') }}</th>
                  <th>{{ $t('about.apiMethod') }}</th>
                  <th>{{ $t('about.apiDescription') }}</th>
                </tr>
              </thead>
              <tbody>
                <tr>
                  <td>{{ $t('about.apiBucketOps') }}</td>
                  <td>CreateBucket</td>
                  <td>{{ $t('about.apiCreateBucket') }}</td>
                </tr>
                <tr>
                  <td>{{ $t('about.apiBucketOps') }}</td>
                  <td>HeadBucket</td>
                  <td>{{ $t('about.apiHeadBucket') }}</td>
                </tr>
                <tr>
                  <td>{{ $t('about.apiBucketOps') }}</td>
                  <td>ListBuckets</td>
                  <td>{{ $t('about.apiListBuckets') }}</td>
                </tr>
                <tr>
                  <td>{{ $t('about.apiBucketOps') }}</td>
                  <td>DeleteBucket</td>
                  <td>{{ $t('about.apiDeleteBucket') }}</td>
                </tr>
                <tr>
                  <td>{{ $t('about.apiObjectOps') }}</td>
                  <td>PutObject</td>
                  <td>{{ $t('about.apiPutObject') }}</td>
                </tr>
                <tr>
                  <td>{{ $t('about.apiObjectOps') }}</td>
                  <td>GetObject</td>
                  <td>{{ $t('about.apiGetObject') }}</td>
                </tr>
                <tr>
                  <td>{{ $t('about.apiObjectOps') }}</td>
                  <td>HeadObject</td>
                  <td>{{ $t('about.apiHeadObject') }}</td>
                </tr>
                <tr>
                  <td>{{ $t('about.apiObjectOps') }}</td>
                  <td>DeleteObject</td>
                  <td>{{ $t('about.apiDeleteObject') }}</td>
                </tr>
                <tr>
                  <td>{{ $t('about.apiMultipart') }}</td>
                  <td>CreateMultipartUpload</td>
                  <td>{{ $t('about.apiCreateMultipartUpload') }}</td>
                </tr>
                <tr>
                  <td>{{ $t('about.apiMultipart') }}</td>
                  <td>UploadPart</td>
                  <td>{{ $t('about.apiUploadPart') }}</td>
                </tr>
                <tr>
                  <td>{{ $t('about.apiMultipart') }}</td>
                  <td>CompleteMultipartUpload</td>
                  <td>{{ $t('about.apiCompleteMultipartUpload') }}</td>
                </tr>
              </tbody>
            </table>
          </div>
        </section>

        <!-- Performance Optimization -->
        <section id="performance" class="about-section" style="background: var(--gray-100);">
          <div class="container">
            <h2 class="section-title">{{ $t('about.performanceTitle') }}</h2>
            <div class="performance-metrics">
              <div class="metric-card">
                <div class="metric-value">90%+</div>
                <div class="metric-label">{{ $t('about.performanceMetricStorage') }}</div>
              </div>
              <div class="metric-card">
                <div class="metric-value">13.5GB/s</div>
                <div class="metric-label">{{ $t('about.performanceMetricUltraCDC') }}</div>
              </div>
              <div class="metric-card">
                <div class="metric-value">9GB/s</div>
                <div class="metric-label">{{ $t('about.performanceMetricFastCDC') }}</div>
              </div>
              <div class="metric-card">
                <div class="metric-value">~16KB</div>
                <div class="metric-label">{{ $t('about.performanceMetricChunking') }}</div>
              </div>
            </div>

            <div style="margin-top: 2rem;">
              <h3>{{ $t('about.performanceStrategyTitle') }}</h3>
              <div class="features-grid">
                <div class="feature-card">
                  <h3 class="feature-title">{{ $t('about.performanceStrategy1Title') }}</h3>
                  <p class="feature-desc">{{ $t('about.performanceStrategy1Desc') }}</p>
                </div>
                <div class="feature-card">
                  <h3 class="feature-title">{{ $t('about.performanceStrategy2Title') }}</h3>
                  <p class="feature-desc">{{ $t('about.performanceStrategy2Desc') }}</p>
                </div>
                <div class="feature-card">
                  <h3 class="feature-title">{{ $t('about.performanceStrategy3Title') }}</h3>
                  <p class="feature-desc">{{ $t('about.performanceStrategy3Desc') }}</p>
                </div>
              </div>
            </div>
          </div>
        </section>

        <!-- License -->
        <section id="license" class="about-section">
          <div class="container">
            <h2 class="section-title">{{ $t('about.licenseTitle') }}</h2>
            <div class="license-info">
              <h3>GNU General Public License v3.0</h3>
              <p>{{ $t('about.licenseDesc') }}</p>

              <div class="license-terms">
                <div class="term-item">
                  <div class="metric-value">✅</div>
                  <div>{{ $t('about.licenseTerm1') }}</div>
                </div>
                <div class="term-item">
                  <div class="metric-value">✅</div>
                  <div>{{ $t('about.licenseTerm2') }}</div>
                </div>
                <div class="term-item">
                  <div class="metric-value">✅</div>
                  <div>{{ $t('about.licenseTerm3') }}</div>
                </div>
                <div class="term-item">
                  <div class="metric-value">🔗</div>
                  <div>{{ $t('about.licenseTerm4') }}</div>
                </div>
              </div>
            </div>
          </div>
        </section>

        <!-- Acknowledgments -->
        <section class="about-section" style="background: var(--gray-100);">
          <div class="container">
            <h2 class="section-title">{{ $t('about.acknowledgmentsTitle') }}</h2>
            <p>{{ $t('about.acknowledgmentsDesc') }}</p>
            <div class="acknowledgments">
              <div class="acknowledgment-item">
                <div class="acknowledgment-name">BadgerDB</div>
                <div>{{ $t('about.acknowledgmentBadgerDB') }}</div>
              </div>
              <div class="acknowledgment-item">
                <div class="acknowledgment-name">TiKV</div>
                <div>{{ $t('about.acknowledgmentTiKV') }}</div>
              </div>
              <div class="acknowledgment-item">
                <div class="acknowledgment-name">fastcdc</div>
                <div>{{ $t('about.acknowledgmentFastcdc') }}</div>
              </div>
              <div class="acknowledgment-item">
                <div class="acknowledgment-name">AWS SDK</div>
                <div>{{ $t('about.acknowledgmentAWSSDK') }}</div>
              </div>
            </div>
          </div>
        </section>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed } from 'vue'
import { useI18n } from 'vue-i18n'
const { locale } = useI18n();

// 获取当前语言
const currentLanguage = computed(() => {
  return locale.value;
});

// 一次性加载所有 diagram-*.svg 文件，返回 { [path]: () => Promise<module> }
const diagramModules = import.meta.glob('@/assets/img/diagram-*.svg', { eager: true });

const getDiagramUrl = (lang) => {
  console.log('diagramModules', diagramModules);
  const key = `/assets/img/diagram-${lang}.svg`; // 注意：Vite glob 返回的 key 是以 /src/ 开头的绝对路径
  const mod = diagramModules[key];
  if (mod) {
    // eager 模式下，mod.default 就是 URL 字符串
    return mod.default;
  }
  // fallback to default language, e.g., 'en'
  return diagramModules['/assets/img/diagram-en.svg']?.default || '';
};

// 架构图图片路径
const architectureImagePath = computed(() => {
  return getDiagramUrl(locale.value);
});
</script>

<style scoped>
.about-container {
  padding: 1.25rem;
  min-height: 100vh;
  background-color: #f8fafc;
}

.about-header {
  margin-bottom: 1.25rem;
}

.about-content {
  background: white;
  border-radius: 0.75rem;
  box-shadow: 0 0.25rem 0.375rem -0.0625rem rgba(0, 0, 0, 0.1), 0 0.125rem 0.25rem -0.0625rem rgba(0, 0, 0, 0.06);
  padding: 0;
}

.about-inner {
  /* 重置内部样式以避免冲突 */
}

/* 重置一些可能与主应用冲突的样式 */
.about-inner * {
  box-sizing: border-box;
}

/* 以下是 about.html 中定义的样式，稍作调整以适应Vue组件环境 */
.about-page-header {
  background: linear-gradient(135deg, #2563eb, #1d4ed8);
  color: white;
  padding: 2rem 0;
  text-align: center;
}

.about-page-header .container {
  max-width: 50rem;
  margin: 0 auto;
}

.about-page-header h1 {
  font-size: 2.5rem;
  margin-bottom: 1rem;
  font-weight: 700;
}

.about-page-header .subtitle {
  font-size: 1.25rem;
  opacity: 0.9;
  margin-bottom: 1.5rem;
}

.about-page-header .tagline {
  background: rgba(255, 255, 255, 0.15);
  display: inline-block;
  padding: 0.5rem 1.5rem;
  border-radius: 3.125rem;
  font-size: 0.9rem;
  margin-top: 1rem;
}

.about-navigation {
  background: white;
  box-shadow: 0 0.25rem 0.375rem -0.0625rem rgba(0, 0, 0, 0.1), 0 0.125rem 0.25rem -0.0625rem rgba(0, 0, 0, 0.06);
  position: sticky;
  top: 0;
  z-index: 100;
}

.about-navigation .container {
  display: flex;
  justify-content: center;
  flex-wrap: wrap;
  padding: 1rem 0;
}

.about-navigation .nav-link {
  color: #334155;
  text-decoration: none;
  padding: 0.5rem 1rem;
  margin: 0.25rem;
  border-radius: 0.375rem;
  transition: all 0.3s ease;
}

.about-navigation .nav-link:hover,
.about-navigation .nav-link.active {
  background-color: #2563eb;
  color: white;
}

.about-section {
  padding: 4rem 0;
}

.about-section .container {
  max-width: 75rem;
  margin: 0 auto;
  padding: 0 1.5rem;
}

.about-section .section-title {
  text-align: center;
  font-size: 2rem;
  margin-bottom: 3rem;
  color: #0f172a;
  position: relative;
}

.about-section .section-title::after {
  content: '';
  position: absolute;
  bottom: -0.625rem;
  left: 50%;
  transform: translateX(-50%);
  width: 3.75rem;
  height: 0.1875rem;
  background: #2563eb;
}

.features-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(18.75rem, 1fr));
  gap: 2rem;
  margin-bottom: 3rem;
}

.feature-card {
  background: white;
  padding: 2rem;
  border-radius: 0.75rem;
  box-shadow: 0 0.25rem 0.375rem -0.0625rem rgba(0, 0, 0, 0.1), 0 0.125rem 0.25rem -0.0625rem rgba(0, 0, 0, 0.06);
  transition: transform 0.3s ease, box-shadow 0.3s ease;
  text-align: center;
}

.feature-card:hover {
  transform: translateY(-0.3125rem);
  box-shadow: 0 0.625rem 1.5625rem -0.3125rem rgba(0, 0, 0, 0.1);
}

.feature-icon {
  width: 4rem;
  height: 4rem;
  background: linear-gradient(135deg, #2563eb, #1d4ed8);
  border-radius: 50%;
  display: flex;
  align-items: center;
  justify-content: center;
  font-size: 2rem;
}

/* 优化策略标题样式 */
.performance-metrics+div h3 {
  text-align: center;
  font-size: 1.5rem;
  margin-bottom: 1.5rem;
  margin-top: 1rem;
  color: #0f172a;
}

.feature-title {
  font-size: 1.25rem;
  margin-bottom: 1rem;
  color: #0f172a;
}

.feature-desc {
  color: #64748b;
  line-height: 1.7;
}

.architecture {
  background: white;
  padding: 3rem;
  border-radius: 0.75rem;
  box-shadow: 0 0.25rem 0.375rem -0.0625rem rgba(0, 0, 0, 0.1), 0 0.125rem 0.25rem -0.0625rem rgba(0, 0, 0, 0.06);
  margin: 2rem 0;
}

.architecture h3 {
  text-align: center;
  font-size: 1.5rem;
  margin-bottom: 1.5rem;
  color: #0f172a;
}

.architecture p {
  text-align: center;
  color: #64748b;
  line-height: 1.7;
  margin-bottom: 2rem;
}

/* 鸣谢部分段落文本样式 */
.section-title+p {
  text-align: center;
  color: #64748b;
  line-height: 1.7;
  font-size: 1.1rem;
  margin-bottom: 2rem;
  font-weight: 400;
}

.license-info h3 {
  text-align: center;
  font-size: 1.5rem;
  margin-bottom: 1.5rem;
  color: #0f172a;
}

.license-info p {
  text-align: center;
  color: #64748b;
  line-height: 1.7;
  margin-bottom: 2rem;
}

.architecture-diagram {
  display: flex;
  align-items: center;
  justify-content: center;
  margin: 2rem auto;
  padding: 1.25rem;
  width: 100%;
  max-width: 56.25rem;
  overflow: hidden;
}

.architecture-image {
  max-width: 100%;
  height: auto;
  display: block;
  object-fit: contain;
}



.api-table {
  width: 100%;
  border-collapse: collapse;
  background: white;
  border-radius: 0.5rem;
  overflow: hidden;
  box-shadow: 0 0.25rem 0.375rem -0.0625rem rgba(0, 0, 0, 0.1), 0 0.125rem 0.25rem -0.0625rem rgba(0, 0, 0, 0.06);
}

.api-table th,
.api-table td {
  padding: 1rem;
  text-align: left;
  border-bottom: 0.0625rem solid #e2e8f0;
}

.api-table th {
  background: #f1f5f9;
  font-weight: 600;
  color: #334155;
}

.api-table tr:hover {
  background: #f8fafc;
}

.performance-metrics {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(9.375rem, 1fr));
  gap: 1.5rem;
  margin: 2rem 0;
}

.metric-card {
  background: white;
  padding: 1.5rem;
  border-radius: 0.5rem;
  box-shadow: 0 0.25rem 0.375rem -0.0625rem rgba(0, 0, 0, 0.1), 0 0.125rem 0.25rem -0.0625rem rgba(0, 0, 0, 0.06);
  text-align: center;
}

.metric-value {
  font-size: 2.5rem;
  font-weight: 700;
  color: #2563eb;
  margin-bottom: 0.5rem;
}

.metric-label {
  color: #64748b;
  font-size: 0.9rem;
}

.license-info {
  background: white;
  padding: 2rem;
  border-radius: 0.5rem;
  box-shadow: 0 0.25rem 0.375rem -0.0625rem rgba(0, 0, 0, 0.1), 0 0.125rem 0.25rem -0.0625rem rgba(0, 0, 0, 0.06);
  margin: 2rem 0;
}

.license-terms {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(9.375rem, 1fr));
  gap: 1rem;
  margin-top: 1.5rem;
}

.term-item {
  background: #f1f5f9;
  padding: 1rem;
  border-radius: 0.375rem;
  text-align: center;
}

.acknowledgments {
  display: flex;
  flex-wrap: wrap;
  gap: 1rem;
  justify-content: center;
  margin-top: 2rem;
}

.acknowledgment-item {
  background: white;
  padding: 1rem;
  border-radius: 0.5rem;
  box-shadow: 0 0.25rem 0.375rem -0.0625rem rgba(0, 0, 0, 0.1), 0 0.125rem 0.25rem -0.0625rem rgba(0, 0, 0, 0.06);
  text-align: center;
  min-width: 9.375rem;
}

.acknowledgment-name {
  font-weight: 600;
  margin-bottom: 0.5rem;
  color: #2563eb;
}

/* 响应式设计 */
@media (max-width: 48rem) {
  .about-page-header h1 {
    font-size: 2rem;
  }

  .about-section .section-title {
    font-size: 1.5rem;
  }

  .architecture {
    padding: 1.5rem;
  }

  .about-navigation .container {
    flex-direction: column;
    align-items: center;
  }
}
</style>