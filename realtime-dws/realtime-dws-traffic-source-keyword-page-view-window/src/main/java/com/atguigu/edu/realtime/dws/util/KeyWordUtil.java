package com.atguigu.edu.realtime.dws.util;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * ClassName: KeyWordUtil
 * Package: com.atguigu.edu.realtime.dws.util
 * Description:
 *
 * @Author: tubage
 * @Create: 2024/5/7 14:51
 * @Version: 1.0
 */
public class KeyWordUtil {
    public static List<String> analyze(String text) {
        List<String> wordList = new ArrayList<>();

        StringReader reader = new StringReader(text);
        IKSegmenter ik = new IKSegmenter(reader, true);


        try {
            Lexeme lexeme = null;
            while ((lexeme = ik.next()) != null) {
                String lexemeText = lexeme.getLexemeText();
                wordList.add(lexemeText);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return wordList;
    }

    public static void main(String[] args) {
        List<String> list = analyze("小米手机京东自营");
        System.out.println(list);
    }
}

