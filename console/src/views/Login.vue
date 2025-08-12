<template>
    <div class="login-page">
        <div class="background-elements">
            <div class="grid-pattern"></div>
            <div class="radial-gradient"></div>
            <div class="polygon polygon-1"></div>
            <div class="polygon polygon-2"></div>
            <div class="polygon polygon-3"></div>
            <div class="polygon polygon-4"></div>
            <div class="tech-line line-1"></div>
            <div class="tech-line line-2"></div>
        </div>

        <div class="content-wrapper">
            <!-- 介绍区域（PC端显示） -->
            <div class="intro-section">
                <div class="logo">
                    <div class="logo-icon">
                        <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2"
                                d="M5 12h14M5 12a2 2 0 01-2-2V6a2 2 0 012-2h14a2 2 0 012 2v4a2 2 0 01-2 2M5 12a2 2 0 00-2 2v4a2 2 0 002 2h14a2 2 0 002-2v-4a2 2 0 00-2-2m-2-4h.01M17 16h.01" />
                        </svg>
                    </div>
                    <div class="logo-text">磐石 (面向对象存储)</div>
                </div>

                <h1 class="tagline">高级云存储<span>管理平台</span></h1>
                <p class="subtitle">通过我们直观的浏览器界面安全访问和管理您的S3兼容存储。企业级安全性与用户友好设计相结合。</p>

                <div class="features">
                    <div class="feature flex flex-col items-center">
                        <div class="flex items-center w-full gap-x-4">
                            <div class="feature-icon">
                                <i class="fas fa-shield-alt"></i>
                            </div>
                            <div class="feature-header">
                                <div class="feature-title">端到端加密凭证</div>
                            </div>
                        </div>
                        <div class="feature-content">
                            <p class="feature-description">
                                所有凭证在传输和存储过程中都经过AES-256加密处理。
                            </p>
                        </div>
                    </div>

                    <div class="feature flex flex-col items-center">
                        <div class="flex items-center w-full gap-x-4">
                            <div class="feature-icon">
                                <i class="fas fa-bolt"></i>
                            </div>
                            <div class="feature-header">
                                <div class="feature-title">闪电般传输速度</div>
                            </div>
                        </div>

                        <div class="feature-content">
                            <p class="feature-description">
                                使用多线程传输和智能分块技术，支持断点续传和并行上传。
                            </p>
                        </div>
                    </div>

                    <div class="feature flex flex-col items-center">
                        <div class="flex items-center w-full gap-x-4">
                            <div class="feature-icon">
                                <i class="fas fa-globe-americas"></i>
                            </div>
                            <div class="feature-header">
                                <div class="feature-title">多区域支持</div>
                            </div>
                        </div>

                        <div class="feature-content">

                            <p class="feature-description">
                                支持多区域。智能路由选择确保您始终连接到最近的服务器节点。
                            </p>
                        </div>
                    </div>

                    <div class="feature flex flex-col items-center">
                        <div class="flex items-center w-full gap-x-4">
                            <div class="feature-icon">
                                <i class="fas fa-sync-alt"></i>
                            </div>
                            <div class="feature-header">
                                <div class="feature-title">跨设备同步</div>
                            </div>
                        </div>

                        <div class="feature-content">
                            <p class="feature-description">
                                在任何设备上访问您的最新文件，实时同步确保数据一致性。
                            </p>
                        </div>
                    </div>
                </div>

                <div class="tech-logos">
                    <div class="tech-logo">
                        <svg width="32" height="32" viewBox="0 0 256 198" fill="none"
                            xmlns="http://www.w3.org/2000/svg">
                            <path
                                d="M240.258 197.122H15.741C7.049 197.122 0 190.073 0 181.381V15.741C0 7.049 7.049 0 15.741 0H240.258C248.951 0 256 7.049 256 15.741V181.38C256 190.073 248.951 197.122 240.258 197.122Z"
                                fill="#232F3E" />
                            <path
                                d="M118.412 145.388C118.412 145.388 92.843 149.915 85.333 124.346C77.824 98.777 105.3 75.854 113.94 70.064C122.58 64.273 128.371 57.764 134.161 58.985C139.951 60.206 140.542 64.273 138.701 67.621C136.86 70.97 119.593 87.526 119.593 87.526C119.593 87.526 146.069 64.273 159.212 63.392C172.355 62.511 179.574 69.73 182.332 76.358C185.09 82.987 185.09 90.206 181.741 94.273C178.393 98.34 172.355 101.108 172.355 101.108L118.412 145.388Z"
                                fill="white" />
                            <path
                                d="M116.571 110.471C116.571 110.471 100.015 113.239 95.948 102.497C91.881 91.755 109.738 79.119 116.571 76.358C123.404 73.597 128.371 70.064 131.72 71.285C135.068 72.506 134.477 75.854 133.256 77.695C132.035 79.536 119.593 88.116 119.593 88.116L116.571 110.471Z"
                                fill="#FF9900" />
                        </svg>
                    </div>
                    <div class="tech-logo">
                        <svg width="32" height="32" viewBox="0 0 256 315" fill="none"
                            xmlns="http://www.w3.org/2000/svg">
                            <path d="M130.003 313.607L0 236.722V78.98L130.003 1.724L256 78.354V236.722L130.003 313.607Z"
                                fill="#00C4B3" />
                            <path d="M130.003 313.607L256 236.722V78.98L130.003 155.865V313.607Z" fill="#00C4B3" />
                            <path d="M130.003 155.865L0 78.98L130.003 1.724V155.865Z" fill="#00A98F" />
                        </svg>
                    </div>
                    <div class="tech-logo">
                        <svg width="32" height="32" viewBox="0 0 256 222" fill="none"
                            xmlns="http://www.w3.org/2000/svg">
                            <path
                                d="M127.693 221.704L0 166.557V55.161L127.693 0L255.385 55.161V166.557L127.693 221.704Z"
                                fill="#0052CC" />
                            <path d="M127.693 221.704L255.385 166.557V55.161L127.693 110.308V221.704Z" fill="#2684FF" />
                            <path d="M127.693 110.308L0 55.161L127.693 0V110.308Z" fill="#2684FF" />
                        </svg>
                    </div>
                </div>
            </div>

            <!-- 登录表单区域 -->
            <div class="login-section">
                <div class="login-card">
                    <div class="login-header">
                        <div class="login-icon">
                            <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24"
                                stroke="currentColor">
                                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2"
                                    d="M9 3v2m6-2v2M9 19v2m6-2v2M5 9H3m2 6H3m18-6h-2m2 6h-2M7 19h10a2 2 0 002-2V7a2 2 0 00-2-2H7a2 2 0 00-2 2v10a2 2 0 002 2zM9 9h6v6H9V9z" />
                            </svg>
                        </div>
                        <h2 class="login-title">安全连接</h2>
                        <p class="login-subtitle">选择登录方式以继续</p>
                    </div>

                    <!-- 登录方式切换 -->
                    <div class="login-method-toggle">
                        <div :class="['method-btn', { active: loginMethod === 'iam' }]" @click="loginMethod = 'iam'">
                            用户名密码登录
                        </div>
                        <div :class="['method-btn', { active: loginMethod === 's3' }]" @click="loginMethod = 's3'">
                            S3凭证登录
                        </div>
                    </div>

                    <el-form ref="loginFormRef" :model="loginForm" :rules="rules" @submit.prevent="handleLogin"
                        label-position="top">
                        <!-- S3凭证登录表单 -->
                        <div v-if="loginMethod === 's3'">
                            <el-form-item label="Access Key ID" prop="accessKeyId">
                                <el-input v-model="loginForm.accessKeyId" placeholder="AKIAIOSFODNN7EXAMPLE"
                                    size="large" />
                            </el-form-item>

                            <el-form-item label="Secret Access Key" prop="secretAccessKey">
                                <el-input v-model="loginForm.secretAccessKey" type="password"
                                    placeholder="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" size="large" show-password />
                            </el-form-item>

                            <el-form-item label="Endpoint" prop="endpoint">
                                <el-input v-model="loginForm.endpoint" placeholder="https://s3.amazonaws.com 或自定义端点"
                                    size="large" />
                            </el-form-item>

                            <el-form-item label="Region(可选)">
                                <el-select v-model="loginForm.region" placeholder="选择区域" size="large"
                                    style="width: 100%">
                                    <el-option label="美国东部 (弗吉尼亚北部)" value="us-east-1" />
                                    <el-option label="美国东部 (俄亥俄州)" value="us-east-2" />
                                    <el-option label="美国西部 (俄勒冈州)" value="us-west-2" />
                                    <el-option label="欧洲 (爱尔兰)" value="eu-west-1" />
                                    <el-option label="亚太地区 (新加坡)" value="ap-southeast-1" />
                                    <el-option label="自定义" value="custom" />
                                </el-select>
                            </el-form-item>

                            <!-- 自定义区域输入框 -->
                            <el-form-item v-if="loginForm.region === 'custom'" label="自定义区域" prop="customRegion">
                                <el-input v-model="loginForm.customRegion" placeholder="输入自定义区域名称" size="large" />
                            </el-form-item>
                        </div>

                        <!-- 用户名密码登录表单 -->
                        <div v-if="loginMethod === 'iam'">
                            <el-form-item label="用户名" prop="username">
                                <el-input v-model="loginForm.username" placeholder="请输入用户名" size="large" />
                            </el-form-item>

                            <el-form-item label="密码" prop="password">
                                <el-input v-model="loginForm.password" type="password" placeholder="请输入密码" size="large"
                                    show-password />
                            </el-form-item>

                            <div class="remember-forgot">
                                <div class="remember-me">
                                    <input type="checkbox" id="remember" v-model="loginForm.remember">
                                    <label for="remember">记住我</label>
                                </div>
                                <a href="#" class="forgot-password">忘记密码?</a>
                            </div>
                        </div>

                        <el-form-item>
                            <el-button type="primary" @click="handleLogin" :loading="loading" size="large">
                                {{ loading ? '连接中...' : loginMethod === 's3' ? '连接到 S3' : '登录系统' }}
                            </el-button>
                        </el-form-item>
                    </el-form>
                </div>
            </div>
        </div>

    </div>
</template>

<script setup>
import { ref, reactive, watch } from 'vue';
import { ElMessage } from 'element-plus';

const loginFormRef = ref(null);
const loading = ref(false);
const loginMethod = ref('iam'); // 's3' 或 'iam'

const loginForm = reactive({
    accessKeyId: '',
    secretAccessKey: '',
    endpoint: '',
    region: 'us-east-1',
    customRegion: '',
    username: '',
    password: '',
    remember: false
});

const rules = {
    accessKeyId: [
        { required: true, message: '请输入Access Key ID', trigger: 'blur' }
    ],
    secretAccessKey: [
        { required: true, message: '请输入Secret Access Key', trigger: 'blur' }
    ],
    endpoint: [
        { required: true, message: '请输入 endpoint', trigger: 'blur' }
    ],
    customRegion: [
        { required: true, message: '请输入自定义区域', trigger: 'blur' }
    ],
    username: [
        { required: true, message: '请输入用户名', trigger: 'blur' }
    ],
    password: [
        { required: true, message: '请输入密码', trigger: 'blur' },
        { min: 6, message: '密码长度不能少于6位', trigger: 'blur' }
    ]
};

// 监听region变化，当选择custom时清空自定义区域验证
watch(
    () => loginForm.region,
    (newValue) => {
        if (newValue !== 'custom') {
            // 重置自定义区域值
            loginForm.customRegion = '';
            // 如果有验证错误，清除它
            if (loginFormRef.value) {
                loginFormRef.value.clearValidate('customRegion');
            }
        }
    }
);

// 切换登录方式时重置表单验证
watch(loginMethod, () => {
    if (loginFormRef.value) {
        loginFormRef.value.clearValidate();
    }
});

const handleLogin = async () => {
    if (!loginFormRef.value) return;

    try {
        const valid = await loginFormRef.value.validate();
        if (!valid) return;

        loading.value = true;

        // 模拟API调用
        await new Promise(resolve => setTimeout(resolve, 1500));

        // 成功消息
        const method = loginMethod.value === 's3' ? 'S3' : 'iam';
        ElMessage.success(`${method}登录成功！正在跳转...`);

        // 模拟跳转
        setTimeout(() => {
            // 实际项目中这里应该是路由跳转
            console.log(`Redirecting to ${method} dashboard...`);
        }, 1000);

    } catch (error) {
        ElMessage.error('登录失败: ' + error.message);
    } finally {
        loading.value = false;
    }
};
</script>

<!-- 全局变量：放在非 scoped 中 -->
<style>
:root {
    --primary-gradient: linear-gradient(135deg, #6e8efb, #a777e3);
    --primary-light: #f0f5ff;
    --card-bg: rgba(255, 255, 255, 0.85);
    --accent-blue: #4f6df5;
    --accent-purple: #a777e3;
    --accent-cyan: #00c6fb;
    --text-dark: #2d3748;
    --text-muted: #718096;
    --border-light: rgba(203, 213, 225, 0.5);
}
</style>

<style scoped>
.login-page {
    min-height: 100vh;
    display: flex;
    align-items: center;
    justify-content: center;
    padding: 2rem;
    position: relative;
    overflow: hidden;
    background: linear-gradient(135deg, #f0f5ff 0%, #f8fafc 100%);
    font-family: 'Inter', 'PingFang SC', 'Microsoft YaHei', sans-serif;
}

/* Background effects */
.background-elements {
    position: absolute;
    top: 0;
    left: 0;
    width: 100%;
    height: 100%;
    z-index: 0;
    overflow: hidden;
}

.grid-pattern {
    position: absolute;
    top: 0;
    left: 0;
    width: 100%;
    height: 100%;
    background-image:
        linear-gradient(rgba(179, 196, 255, 0.1) 1px, transparent 1px),
        linear-gradient(90deg, rgba(179, 196, 255, 0.1) 1px, transparent 1px);
    background-size: 40px 40px;
    opacity: 0.6;
}

.radial-gradient {
    position: absolute;
    top: -50%;
    left: -50%;
    width: 200%;
    height: 200%;
    background: radial-gradient(circle at center, rgba(110, 142, 251, 0.1) 0%, transparent 70%);
    opacity: 0.4;
}

.polygon {
    position: absolute;
    opacity: 0.15;
    filter: blur(30px);
}

.polygon-1 {
    width: 300px;
    height: 300px;
    background: var(--accent-blue);
    top: 15%;
    left: 25%;
    clip-path: polygon(50% 0%, 100% 50%, 50% 100%, 0% 50%);
    animation: float 15s ease-in-out infinite;
}

.polygon-2 {
    width: 250px;
    height: 250px;
    background: var(--accent-purple);
    bottom: 15%;
    right: 10%;
    clip-path: polygon(50% 0%, 100% 38%, 82% 100%, 18% 100%, 0% 38%);
    animation: float 18s ease-in-out infinite;
    animation-delay: 2s;
}

.polygon-3 {
    width: 200px;
    height: 200px;
    background: var(--accent-cyan);
    top: 40%;
    right: 20%;
    clip-path: polygon(20% 0%, 80% 0%, 100% 20%, 100% 80%, 80% 100%, 20% 100%, 0% 80%, 0% 20%);
    animation: float 12s ease-in-out infinite;
    animation-delay: 4s;
}

.polygon-4 {
    width: 350px;
    height: 350px;
    background: var(--accent-purple);
    bottom: 10%;
    left: 15%;
    clip-path: polygon(0% 15%, 15% 15%, 15% 0%, 85% 0%, 85% 15%, 100% 15%, 100% 85%, 85% 85%, 85% 100%, 15% 100%, 15% 85%, 0% 85%);
    animation: float 20s ease-in-out infinite;
    animation-delay: 1s;
}

@keyframes float {
    0% {
        transform: translate(0, 0) rotate(0deg);
    }

    33% {
        transform: translate(20px, -50px) rotate(60deg);
    }

    66% {
        transform: translate(-30px, 30px) rotate(120deg);
    }

    100% {
        transform: translate(0, 0) rotate(180deg);
    }
}

/* Tech lines */
.tech-line {
    position: absolute;
    background: linear-gradient(to right, var(--accent-blue), var(--accent-purple));
    opacity: 0.1;
    transform-origin: left;
}

.line-1 {
    width: 80%;
    height: 2px;
    top: 30%;
    left: 0;
    transform: rotate(-20deg);
}

.line-2 {
    width: 70%;
    height: 2px;
    bottom: 20%;
    right: 0;
    transform: rotate(25deg);
}

/* Content layout */
.content-wrapper {
    display: flex;
    max-width: 1200px;
    width: 100%;
    gap: 40px;
    z-index: 10;
}

.intro-section {
    flex: 1;
    padding: 40px;
    display: flex;
    flex-direction: column;
    justify-content: center;
}

.login-section {
    flex: 1;
    display: flex;
    justify-content: center;
    align-items: center;
}

/* Intro styles */
.logo {
    display: flex;
    align-items: center;
    gap: 12px;
    margin-bottom: 30px;
}

.logo-icon {
    width: 48px;
    height: 48px;
    background: var(--primary-gradient);
    border-radius: 12px;
    display: flex;
    align-items: center;
    justify-content: center;
    box-shadow: 0 4px 10px rgba(110, 142, 251, 0.3);
}

.logo-icon svg {
    width: 28px;
    height: 28px;
    color: white;
}

.logo-text {
    font-size: 24px;
    font-weight: 700;
    background: linear-gradient(to right, #6e8efb, #a777e3);
    background-clip: text;
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
}

.tagline {
    font-size: 42px;
    font-weight: 700;
    line-height: 1.2;
    margin-bottom: 20px;
    color: var(--text-dark);
}

.tagline span {
    background: linear-gradient(to right, #6e8efb, #a777e3);
    background-clip: text;
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
}

.subtitle {
    font-size: 18px;
    color: var(--text-muted);
    margin-bottom: 40px;
    max-width: 500px;
    line-height: 1.6;
}

.features {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 20px;
    margin-bottom: 40px;
}

.feature {
    display: flex;
    gap: 18px;
    background: rgba(255, 255, 255, 0.85);
    padding: 22px;
    border-radius: 16px;
    backdrop-filter: blur(8px);
    border: 1px solid var(--border-light);
    box-shadow: 0 6px 20px rgba(110, 142, 251, 0.08);
    transition: all 0.3s ease;
    position: relative;
    overflow: hidden;
}

.feature:hover {
    transform: translateY(-5px);
    box-shadow: 0 12px 25px rgba(110, 142, 251, 0.15);
    border-color: rgba(110, 142, 251, 0.3);
}

.feature::before {
    content: '';
    position: absolute;
    top: 0;
    left: 0;
    width: 5px;
    height: 100%;
    background: var(--accent-blue);
}

.feature-icon {
    width: 50px;
    height: 50px;
    background: rgba(110, 142, 251, 0.1);
    border-radius: 12px;
    display: flex;
    align-items: center;
    justify-content: center;
    flex-shrink: 0;
    font-size: 22px;
    color: var(--accent-blue);
}

.feature-content {
    flex: 1;
}

.feature-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
}

.feature-title {
    font-size: 17px;
    font-weight: 600;
    color: var(--text-dark);
}

.feature-status {
    font-size: 12px;
    font-weight: 600;
    padding: 4px 10px;
    border-radius: 20px;
    background: rgba(16, 185, 129, 0.1);
    color: var(--accent-green);
}

.feature-description {
    font-size: 14px;
    color: var(--text-muted);
    line-height: 1.6;
    margin-bottom: 12px;
}

.feature-link {
    display: inline-flex;
    align-items: center;
    color: var(--accent-blue);
    font-size: 13px;
    font-weight: 500;
    text-decoration: none;
    transition: all 0.3s;
}

.feature-link:hover {
    color: var(--accent-purple);
    transform: translateX(3px);
}

.feature-link i {
    margin-left: 5px;
    font-size: 11px;
    transition: all 0.3s;
}

.feature-link:hover i {
    transform: translateX(3px);
}

.tech-logos {
    display: flex;
    gap: 20px;
    align-items: center;
    margin-top: auto;
}

.tech-logo {
    opacity: 0.6;
    transition: opacity 0.3s;
}

.tech-logo:hover {
    opacity: 1;
}

/* Login card */
.login-card {
    color: #010101;
    background: #f1f7fe;
    backdrop-filter: blur(16px);
    border-radius: 16px;
    width: 100%;
    max-width: 480px;
    padding: 40px;
    border: 1px solid var(--border-light);
    box-shadow: 0 25px 50px -12px rgba(0, 0, 0, 0.1);
    position: relative;
    overflow: hidden;
    transition: transform 0.3s, box-shadow 0.3s;
}

.login-card:hover {
    transform: translateY(-5px);
    box-shadow: 0 30px 60px -15px rgba(110, 142, 251, 0.3);
}

.login-card::before {
    content: '';
    position: absolute;
    top: 0;
    left: 0;
    width: 100%;
    height: 4px;
    background: var(--primary-gradient);
}

.login-header {
    text-align: center;
    margin-bottom: 30px;
}

.login-icon {
    width: 80px;
    height: 80px;
    margin: 0 auto 20px;
    background: linear-gradient(135deg, rgba(110, 142, 251, 0.1), rgba(167, 119, 227, 0.1));
    border-radius: 50%;
    display: flex;
    align-items: center;
    justify-content: center;
    position: relative;
}

.login-icon::after {
    content: '';
    position: absolute;
    width: 100%;
    height: 100%;
    border-radius: 50%;
    background: linear-gradient(135deg, rgba(79, 109, 245, 0.4), transparent);
    animation: rotate 6s linear infinite;
}

.login-icon svg {
    width: 40px;
    height: 40px;
    color: var(--accent-blue);
    z-index: 2;
}

@keyframes rotate {
    from {
        transform: rotate(0deg);
    }

    to {
        transform: rotate(360deg);
    }
}

.login-title {
    font-size: 28px;
    font-weight: 700;
    margin-bottom: 8px;
    color: var(--text-dark);
}

.login-subtitle {
    color: var(--text-muted);
    font-size: 15px;
}

/* Login method toggle */
.login-method-toggle {
    display: flex;
    background: rgba(255, 255, 255, 0.7);
    border-radius: 12px;
    padding: 6px;
    margin-bottom: 24px;
    border: 1px solid var(--border-light);
}

.method-btn {
    flex: 1;
    text-align: center;
    padding: 10px;
    border-radius: 8px;
    cursor: pointer;
    font-weight: 500;
    transition: all 0.3s;
    color: var(--text-muted);
}

.method-btn.active {
    background: var(--primary-gradient);
    color: white;
    box-shadow: 0 4px 10px rgba(110, 142, 251, 0.3);
}


/* Additional styles */
.remember-forgot {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 20px;
}

.remember-me {
    display: flex;
    align-items: center;
}

.remember-me input {
    margin-right: 8px;
}

.forgot-password {
    color: var(--accent-blue);
    text-decoration: none;
    font-size: 14px;
    transition: color 0.3s;
}

.forgot-password:hover {
    color: var(--accent-purple);
    text-decoration: underline;
}

/* Form elements */
:deep(.el-form-item) {
    margin-bottom: 24px;
}

:deep(.el-form-item__label) {
    color: var(--text-dark);
    font-weight: 500;
    margin-bottom: 8px;
    font-size: 14px;
    display: block;
}

:deep(.el-input__inner) {
    background: rgba(255, 255, 255, 0.7) !important;
    border: 1px solid var(--border-light) !important;
    color: var(--text-dark) !important;
    border-radius: 10px !important;
    padding: 12px 16px !important;
    height: 48px !important;
    transition: all 0.3s !important;
}

:deep(.el-input__inner:focus) {
    border-color: var(--accent-blue) !important;
    box-shadow: 0 0 0 2px rgba(79, 109, 245, 0.2) !important;
}

:deep(.el-input__inner::placeholder) {
    color: #94a3b8 !important;
}

:deep(.el-button) {
    width: 100%;
    height: 50px;
    border-radius: 10px !important;
    font-weight: 600 !important;
    font-size: 16px !important;
    transition: all 0.3s !important;
    border: none !important;
}

:deep(.el-button--primary) {
    background: var(--primary-gradient) !important;
}

:deep(.el-button--primary:hover) {
    transform: translateY(-2px);
    box-shadow: 0 10px 20px -10px rgba(79, 109, 245, 0.4) !important;
}

:deep(.el-button--primary:active) {
    transform: translateY(0);
}

:deep(.el-select .el-input__inner) {
    padding-right: 40px !important;
}

/* Responsive design */
@media (max-width: 900px) {
    .intro-section {
        display: none;
    }

    .login-section {
        width: 100%;
        max-width: 500px;
    }

    .login-card {
        padding: 30px;
    }
}

@media (max-width: 480px) {
    .login-page {
        padding: 1rem;
    }

    .login-card {
        padding: 25px 20px;
    }

    .login-title {
        font-size: 24px;
    }

    .tagline {
        font-size: 28px;
    }
}
</style>