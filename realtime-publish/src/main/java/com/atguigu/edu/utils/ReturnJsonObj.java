package com.atguigu.edu.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * ClassName: ReturnJsonObj
 * Package: com.atguigu.edu.utils
 * Description:
 *
 * @Author: tubage
 * @Create: 2024/5/8 14:04
 * @Version: 1.0
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ReturnJsonObj {
    private Integer status = 0;
    private String msg = "";
    private Object data;

    @Override
    public String toString() {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.writeValueAsString(this);
        } catch (Exception e) {
            return "{\"code\":\"" + status + "\",\"message\":\"" + msg + "\",\"data\":null}";
        }
    }
}
