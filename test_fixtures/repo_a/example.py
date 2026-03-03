class UserService:
    def get_user(self, user_id: int) -> dict:
        return {"id": user_id, "name": "test"}

    def delete_user(self, user_id: int) -> bool:
        return True

def helper_function(x):
    return x + 1
