<template>
  <div id="app-body">
    <!-- 使用 v-slot 获取 Component，再用 transition 包裹 component -->
    <router-view v-slot="{ Component }">
      <transition name="fade" mode="out-in">
        <component :is="Component" />
      </transition>
    </router-view>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'

onMounted(() => {
  console.log('Component mounted.')
})
</script>

<style scoped>

</style>