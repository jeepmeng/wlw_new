from locust import HttpUser, task, between
import time

class VectorUser(HttpUser):
    wait_time = between(1, 2)

    @task
    def submit_and_poll(self):
        payload = {"text": "物模型"}
        headers = {"Content-Type": "application/json"}

        # 提交任务
        resp = self.client.post("/vector/task", json=payload, headers=headers)

        # ✅ 打印调试信息
        print("📤 状态:", resp.status_code)
        print("📤 返回:", resp.text[:200])  # 避免太长

        if resp.status_code != 200:
            print("❌ 请求失败:", resp.status_code)
            return

        try:
            task_id = resp.json().get("data", {}).get("task_id")
        except Exception as e:
            print("⚠️ JSON 解析失败:", e)
            return

        if not task_id:
            print("⚠️ 没有拿到 task_id")
            return

        # 轮询任务结果
        for _ in range(10):
            res = self.client.get(f"/vector/task_result/{task_id}")
            print("🔁 查询状态:", res.status_code)
            if "vector" in res.text or "results" in res.text:
                print("✅ 成功获取结果")
                break
            time.sleep(1)