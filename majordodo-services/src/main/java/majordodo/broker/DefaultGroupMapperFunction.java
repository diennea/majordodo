/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package majordodo.broker;

import majordodo.task.GroupMapperFunction;

/**
 * Default group mapper function
 *
 * @author enrico.olivelli
 */
public class DefaultGroupMapperFunction implements GroupMapperFunction {

    @Override
    public int getGroup(long taskid, String taskType, String assignerData) {
        if (assignerData == null || assignerData.isEmpty()) {
            return 1;
        } else {
            try {
                return Integer.parseInt(assignerData);
            } catch (NumberFormatException err) {
                return 1;
            }
        }
    }

}
