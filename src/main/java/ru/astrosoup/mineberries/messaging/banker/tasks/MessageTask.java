package ru.astrosoup.mineberries.messaging.banker.tasks;

import lombok.AllArgsConstructor;
import lombok.Getter;
import ru.astrosoup.mineberries.messaging.common.AbstractTask;

@AllArgsConstructor 
public class MessageTask extends AbstractTask<Void> {
    private @Getter final String message;
    private @Getter final String from;
    private @Getter final String to;   
}