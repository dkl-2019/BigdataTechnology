package kaizi.momo_chat.service.impl;

import kaizi.momo_chat.entity.Msg;

import java.io.IOException;
import java.util.List;

/**
 * 聊天消息数据福武
 */

public interface ChatMessageService {

    /**
     * 根据日期、发件人、收件人查询消息
     * @param date 日期
     * @param sender 发件人
     * @param receiver 收件人
     * @return 一个消息列表
     * @throws Exception
     */

    List<Msg> getMessage(String date, String sender, String receiver) throws Exception;

    /**
     * 关闭资源
     */
    void close() throws IOException;
}
